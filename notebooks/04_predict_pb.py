# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Race-Time Predictions (PB Trends + Forecast Backtest)
# MAGIC
# MAGIC Reuses the same Riegel/VDOT/elevation-adjusted formulas from the SQL Server
# MAGIC version, but reads from silver Delta tables and writes to gold Delta tables.
# MAGIC
# MAGIC Computation is done in pandas (collected to driver) since the data is small
# MAGIC (~5K RunBest rows, ~1.5K Activities). The formulas are pure NumPy/SciPy.

# COMMAND ----------

CATALOG = "stravasquad"

# COMMAND ----------

import datetime as dt
import numpy as np
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load prediction formulas

# COMMAND ----------

# Formulas are in lib/prediction_formulas.py in the repo.
# In Databricks, add the repo to the workspace and import, or inline them:
# For portability, we inline the core functions here.

from scipy.optimize import brentq

ELEVATION_FACTOR = 2.0

def riegel(t1, d1, d2, exp=1.06):
    return t1 * (d2 / d1) ** exp

def _vo2_cost(v):
    return -4.60 + 0.182258 * v + 0.000104 * v ** 2

def _vo2_pct(t):
    return 0.8 + 0.1894393 * np.exp(-0.012778 * t) + 0.2989558 * np.exp(-0.1932605 * t)

def compute_vdot(d_m, t_sec):
    t_min = t_sec / 60.0
    return _vo2_cost(d_m / t_min) / _vo2_pct(t_min)

def vdot_time(vdot, d_m):
    def f(t_min):
        return _vo2_cost(d_m / t_min) / _vo2_pct(t_min) - vdot
    return brentq(f, 1.0, 600.0) * 60.0

def elev_riegel(t1, d1, src_elev, d2, tgt_elev):
    pace = t1 / (d1 / 1000)
    flat = pace - ELEVATION_FACTOR * src_elev
    flat_t2 = riegel(flat * (d1 / 1000), d1, d2)
    p2 = flat_t2 / (d2 / 1000) + ELEVATION_FACTOR * tgt_elev
    return p2 * (d2 / 1000)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A — PB Trend Regression

# COMMAND ----------

pdf_best = spark.table(f"{CATALOG}.silver.runbest").toPandas()
pdf_best["start_date"] = pd.to_datetime(pdf_best["start_date"])

today = dt.date.today()
today_ord = today.toordinal()
pb_rows = []

for (aid, dist), grp in pdf_best.groupby(["athlete_id", "name"]):
    grp = grp.dropna(subset=["start_date", "elapsed_time"]).sort_values("start_date")
    if len(grp) < 3:
        continue
    x = grp["start_date"].apply(lambda d: d.toordinal()).to_numpy(dtype=float)
    y = grp["elapsed_time"].to_numpy(dtype=float)
    slope, intercept = np.polyfit(x, y, 1)
    y_pred = slope * x + intercept
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - y.mean()) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
    pred = max(slope * today_ord + intercept, 1.0)
    pb_rows.append({
        "athlete_id": int(aid), "distance_label": dist,
        "predicted_best_sec": float(pred), "prediction_date": today,
        "r_squared": float(r2),
    })

df_pb = spark.createDataFrame(pd.DataFrame(pb_rows))
df_pb.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.pb_prediction")

print(f"gold.pb_prediction: {df_pb.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B — Race Forecast Backtest

# COMMAND ----------

DISTANCE_LABELS = {
    "400m": 400, "1/2 mile": 804.672, "1K": 1000, "1 mile": 1609.344,
    "2 mile": 3218.688, "5K": 5000, "10K": 10000, "15K": 15000,
    "10 mile": 16093.44, "20K": 20000, "Half-Marathon": 21097.5,
    "30K": 30000, "Marathon": 42195,
}

SOURCE_PRIORITY = ["10K", "15K", "5K", "20K", "10 mile", "1 mile", "30K"]

TARGET_RACES = [
    {
        "target": "Half-Marathon", "race_date": dt.date(2025, 10, 26),
        "athletes": {
            6091734:   {"aid": 16255637938, "sec": 6421,  "elev": 21.0, "km": 21.38},
            36978778:  {"aid": 16255837334, "sec": 8486,  "elev": 27.0, "km": 21.62},
            38095302:  {"aid": 16256651852, "sec": 11795, "elev": 74.0, "km": 21.39},
            94062156:  {"aid": 16256197542, "sec": 12343, "elev": 32.9, "km": 22.42},
            94062196:  {"aid": 16255807860, "sec": 8232,  "elev": 16.0, "km": 21.41},
            169950101: {"aid": 16255845381, "sec": 8378,  "elev": 16.0, "km": 21.38},
        },
    },
    {
        "target": "Marathon", "race_date": dt.date(2025, 10, 5),
        "athletes": {
            36978778: {"aid": 16036389587, "sec": 19083, "elev": 446.0, "km": 42.72},
        },
    },
]

pdf_act = spark.table(f"{CATALOG}.silver.activities").toPandas()

pdf_merged = pdf_best.merge(
    pdf_act[["activity_id", "distance_km", "elev_gain_m"]].rename(
        columns={"distance_km": "act_km"}
    ),
    on="activity_id", how="left"
)
pdf_merged["elev_per_km"] = pdf_merged["elev_gain_m"] / pdf_merged["act_km"]

fc_rows = []
for race in TARGET_RACES:
    tgt_label = race["target"]
    tgt_m = DISTANCE_LABELS[tgt_label]
    for athlete_id, info in race["athletes"].items():
        tgt_elev = info["elev"] / info["km"]
        for wk in [8, 4, 2, 1]:
            as_of = race["race_date"] - dt.timedelta(weeks=wk)
            cands = pdf_merged[
                (pdf_merged["athlete_id"] == athlete_id)
                & (pdf_merged["start_date"].dt.date < as_of)
                & (pdf_merged["name"].isin(SOURCE_PRIORITY))
            ]
            if cands.empty:
                continue
            src = None
            for label in SOURCE_PRIORITY:
                rows = cands[cands["name"] == label]
                if not rows.empty:
                    src = rows.nsmallest(1, "elapsed_time").iloc[0]
                    break
            if src is None:
                continue

            src_m = DISTANCE_LABELS[src["name"]]
            src_sec = float(src["elapsed_time"])
            src_elev = float(src["elev_per_km"]) if pd.notna(src["elev_per_km"]) else 0.0

            preds = {
                "riegel": riegel(src_sec, src_m, tgt_m),
                "vdot": vdot_time(compute_vdot(src_m, src_sec), tgt_m),
                "elevation_adjusted": elev_riegel(src_sec, src_m, src_elev, tgt_m, tgt_elev),
            }
            actual = float(info["sec"])
            for method, pred in preds.items():
                fc_rows.append({
                    "athlete_id": int(athlete_id),
                    "race_activity_id": int(info["aid"]),
                    "target_distance_label": tgt_label,
                    "method": method,
                    "as_of_date": as_of,
                    "source_distance_label": src["name"],
                    "source_pb_sec": src_sec,
                    "predicted_sec": float(pred),
                    "actual_sec": actual,
                    "error_sec": float(pred - actual),
                    "error_pct": float((pred - actual) / actual * 100),
                })

df_fc = spark.createDataFrame(pd.DataFrame(fc_rows))
df_fc.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.race_forecast")

print(f"gold.race_forecast: {df_fc.count()} rows")
