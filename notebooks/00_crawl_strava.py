# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Crawl Strava API
# MAGIC
# MAGIC Fetches activity data from the Strava API for all athletes, then writes
# MAGIC consolidated CSVs directly to bronze Delta tables. Replaces the local
# MAGIC pipeline scripts `01_download_streams.py`, `02_export_activities.py`,
# MAGIC and `03_profile_crawl.py`.
# MAGIC
# MAGIC ### Setup (one-time)
# MAGIC Store your Strava credentials and athlete tokens in Databricks Secrets:
# MAGIC ```
# MAGIC databricks secrets create-scope strava
# MAGIC databricks secrets put-secret strava client_id --string-value "YOUR_CLIENT_ID"
# MAGIC databricks secrets put-secret strava client_secret --string-value "YOUR_SECRET"
# MAGIC databricks secrets put-secret strava tokens_json --string-value '{"athlete_id": {...}, ...}'
# MAGIC ```
# MAGIC Or via the Databricks UI: Workspace Settings → Secrets.

# COMMAND ----------

import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Load credentials from Databricks Secrets (never hardcoded in notebooks)
CLIENT_ID = dbutils.secrets.get("strava", "client_id")
CLIENT_SECRET = dbutils.secrets.get("strava", "client_secret")
tokens_raw = dbutils.secrets.get("strava", "tokens_json")
tokens = json.loads(tokens_raw)

DAYS_BACK = 15
PER_PAGE = 200

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def refresh_token(tok):
    """Refresh an expired Strava OAuth access token."""
    if time.time() < tok.get("expires_at", 0) - 300:
        return tok["access_token"]
    resp = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": tok["refresh_token"],
        },
        timeout=15,
    ).json()
    tok.update(
        access_token=resp["access_token"],
        refresh_token=resp["refresh_token"],
        expires_at=resp["expires_at"],
    )
    return tok["access_token"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 — Fetch Activity Streams (GPS, HR, cadence, watts)

# COMMAND ----------

since_ts = int(time.time() - DAYS_BACK * 86400)
all_streams = []
all_segments = []
all_best_efforts = []

# Load existing activity IDs from bronze to skip duplicates
existing_activity_ids = set()
try:
    existing_activity_ids = set(
        spark.table("bronze.runstream")
        .select("activity_id").distinct()
        .toPandas()["activity_id"].astype(str)
    )
    print(f"Found {len(existing_activity_ids):,} existing activity IDs in bronze — will skip these")
except Exception:
    print("No existing bronze.runstream table — all activities are new")

for aid, meta in tokens.items():
    athlete_name = meta.get("name", str(aid))
    print(f"\n=== {athlete_name} ({aid}) ===")
    access = refresh_token(meta)
    hdrs = {"Authorization": f"Bearer {access}"}

    page = 1
    while True:
        params = {"per_page": PER_PAGE, "page": page, "after": since_ts}
        res = requests.get(
            "https://www.strava.com/api/v3/athlete/activities",
            headers=hdrs, params=params, timeout=20,
        )
        if res.status_code != 200:
            print(f"  HTTP {res.status_code}")
            break
        acts = res.json()
        if not isinstance(acts, list) or not acts:
            break

        for a in acts:
            act_id = a["id"]
            sport = a.get("sport_type", "unknown").lower()

            # Skip if already in bronze
            if str(act_id) in existing_activity_ids:
                print(f"  skip {act_id} (already in bronze)")
                continue

            # Fetch streams
            st_res = requests.get(
                f"https://www.strava.com/api/v3/activities/{act_id}/streams",
                headers=hdrs,
                params={
                    "keys": "latlng,time,distance,altitude,heartrate,cadence,watts",
                    "key_by_type": "true",
                },
                timeout=25,
            )
            if st_res.status_code != 200 or "latlng" not in st_res.json():
                print(f"  skip {act_id} (no stream)")
                continue

            st = st_res.json()
            n = len(st["time"]["data"])
            df_stream = pd.DataFrame({
                "time_s":    st["time"]["data"],
                "lat":       [pt[0] for pt in st["latlng"]["data"]],
                "lon":       [pt[1] for pt in st["latlng"]["data"]],
                "dist_m":    st.get("distance", {}).get("data", [None] * n),
                "alt_m":     st.get("altitude", {}).get("data", [None] * n),
                "hr_bpm":    st.get("heartrate", {}).get("data", [None] * n),
                "cadence":   st.get("cadence", {}).get("data", [None] * n),
                "watts":     st.get("watts", {}).get("data", [None] * n),
                "activity_id": act_id,
            })
            all_streams.append(df_stream)

            # Fetch detailed activity (for segments + best efforts)
            det = requests.get(
                f"https://www.strava.com/api/v3/activities/{act_id}",
                headers=hdrs, timeout=25,
            ).json()

            segs = det.get("segment_efforts", [])
            if segs:
                df_seg = pd.DataFrame(segs)
                df_seg["activity_id"] = act_id
                df_seg["athlete_id"] = int(aid)
                all_segments.append(df_seg)

            if sport == "run":
                be = det.get("best_efforts", [])
                if be:
                    df_be = pd.DataFrame(be)
                    df_be["activity_id"] = act_id
                    df_be["athlete_id"] = int(aid)
                    all_best_efforts.append(df_be)

            print(f"  done {act_id} ({sport})")
        page += 1

print(f"\nTotal: {len(all_streams)} activities with streams")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 — Fetch Activity Summaries

# COMMAND ----------

# Load existing activity IDs from bronze.activities
existing_act_ids = set()
try:
    existing_act_ids = set(
        spark.table("bronze.activities")
        .select("activity_id").distinct()
        .toPandas()["activity_id"].astype(str)
    )
except Exception:
    pass

all_activities = []
for aid, meta in tokens.items():
    athlete_id = int(aid)
    athlete_name = meta.get("name", "")
    access = refresh_token(meta)
    hdrs = {"Authorization": f"Bearer {access}"}

    page = 1
    while True:
        resp = requests.get(
            "https://www.strava.com/api/v3/athlete/activities",
            headers=hdrs,
            params={"per_page": PER_PAGE, "page": page, "after": since_ts},
            timeout=20,
        )
        data = resp.json()
        if not isinstance(data, list) or not data:
            break
        for a in data:
            if str(a.get("id")) in existing_act_ids:
                continue
            dist_m = a.get("distance", 0) or 0
            elapsed = a.get("elapsed_time", 0) or 0
            all_activities.append({
                "activity_id": a.get("id"),
                "activity_type": a.get("type"),
                "athlete_id": athlete_id,
                "athlete_name": athlete_name,
                "date": a.get("start_date_local", "")[:10],
                "start_time_utc": a.get("start_date"),
                "duration_sec": elapsed,
                "distance_km": dist_m / 1000,
                "elev_gain_m": a.get("total_elevation_gain", 0),
                "avg_pace_sec_km": elapsed / (dist_m / 1000) if dist_m else None,
                "source": "API",
            })
        page += 1

print(f"Fetched {len(all_activities)} NEW activity summaries (skipped {len(existing_act_ids)} existing)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 — Fetch Athlete Profiles

# COMMAND ----------

all_profiles = []
for aid, meta in tokens.items():
    access = refresh_token(meta)
    resp = requests.get(
        "https://www.strava.com/api/v3/athlete",
        headers={"Authorization": f"Bearer {access}"},
        timeout=15,
    )
    if resp.status_code == 200:
        p = resp.json()
        all_profiles.append({
            "athlete_id": int(aid),
            "firstname": p.get("firstname"),
            "lastname": p.get("lastname"),
            "city": p.get("city"),
            "country": p.get("country"),
            "sex": p.get("sex"),
            "weight": p.get("weight"),
        })

print(f"Fetched {len(all_profiles)} athlete profiles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 — Write to Bronze Delta Tables

# COMMAND ----------

if all_streams:
    pdf = pd.concat(all_streams, ignore_index=True)
    df = spark.createDataFrame(pdf).withColumn("ingested_at", current_timestamp())
    df.write.format("delta").mode("append").saveAsTable("bronze.runstream")
    print(f"bronze.runstream: appended {len(pdf):,} rows")

if all_best_efforts:
    pdf = pd.concat(all_best_efforts, ignore_index=True)
    df = spark.createDataFrame(pdf).withColumn("ingested_at", current_timestamp())
    df.write.format("delta").mode("append").saveAsTable("bronze.runbest")
    print(f"bronze.runbest: appended {len(pdf):,} rows")

if all_segments:
    pdf = pd.concat(all_segments, ignore_index=True)
    df = spark.createDataFrame(pdf).withColumn("ingested_at", current_timestamp())
    df.write.format("delta").mode("append").saveAsTable("bronze.runsegment")
    print(f"bronze.runsegment: appended {len(pdf):,} rows")

if all_activities:
    pdf = pd.DataFrame(all_activities)
    df = spark.createDataFrame(pdf).withColumn("ingested_at", current_timestamp())
    df.write.format("delta").mode("append").saveAsTable("bronze.activities")
    print(f"bronze.activities: appended {len(pdf):,} rows")

if all_profiles:
    pdf = pd.DataFrame(all_profiles)
    df = spark.createDataFrame(pdf).withColumn("ingested_at", current_timestamp())
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable("bronze.athlete")
    print(f"bronze.athlete: overwrote with {len(pdf)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5 — Update Stored Tokens
# MAGIC
# MAGIC After refreshing, the tokens may have changed. Update the secret so the
# MAGIC next run uses the fresh tokens. (Requires secret write permission.)

# COMMAND ----------

try:
    dbutils.secrets.put("strava", "tokens_json", json.dumps(tokens))
    print("Tokens updated in Databricks Secrets")
except Exception as e:
    print(f"Could not update tokens (expected if using UI-managed secrets): {e}")
    print("To update manually, run: databricks secrets put-secret strava tokens_json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

for table in ["runstream", "runbest", "runsegment", "activities", "athlete"]:
    count = spark.table(f"bronze.{table}").count()
    print(f"  bronze.{table}: {count:,} rows")
