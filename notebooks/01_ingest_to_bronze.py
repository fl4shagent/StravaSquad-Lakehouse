# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest CSVs to Bronze (Delta Lake)
# MAGIC
# MAGIC Reads raw CSV exports from S3, adds an `ingested_at` audit timestamp,
# MAGIC and appends to bronze Delta tables. Mirrors the append-only semantics of
# MAGIC the original SQL Server `bronze.*` schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

S3_RAW_PATH = "s3://stravasquad-lakehouse-759869090470-ap-southeast-2-an/raw"
CATALOG = "stravasquad"
BRONZE_SCHEMA = "bronze"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

def ingest_csv_to_bronze(filename, table_name, encoding="utf-8"):
    """Read a CSV from S3 and append to a bronze Delta table."""
    path = f"{S3_RAW_PATH}/{filename}"
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", encoding)
        .csv(path)
    )
    df = df.withColumn("ingested_at", current_timestamp())

    full_table = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    df.write.format("delta").mode("append").saveAsTable(full_table)

    count = spark.table(full_table).count()
    print(f"OK {full_table}: {count} total rows (appended {df.count()})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest all 6 source files

# COMMAND ----------

ingest_csv_to_bronze("runstream.csv",                        "runstream")
ingest_csv_to_bronze("best_clean_datetime.csv",              "runbest")
ingest_csv_to_bronze("segments_clean_datetime.csv",          "runsegment",        encoding="utf-8")
ingest_csv_to_bronze("runstream_segments_by_kilometers.csv", "runsplitkilometer")
ingest_csv_to_bronze("all_activities_clean_datetime.csv",    "activities")
ingest_csv_to_bronze("athletes_profiles_clean.csv",          "athlete",           encoding="utf-8")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify row counts

# COMMAND ----------

for table in ["runstream", "runbest", "runsegment", "runsplitkilometer", "activities", "athlete"]:
    count = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{table}").count()
    print(f"  {table}: {count:,} rows")
