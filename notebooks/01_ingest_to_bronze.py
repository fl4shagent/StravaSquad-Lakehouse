# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest CSVs to Bronze (Delta Lake)
# MAGIC
# MAGIC Reads raw CSV files and writes them to bronze Delta tables with an
# MAGIC `ingested_at` audit timestamp. This is the first step of the medallion
# MAGIC pipeline — bronze is the raw landing zone (append-only).
# MAGIC
# MAGIC **Data source:** CSVs uploaded to a Databricks Volume, or directly via
# MAGIC the `bronze.*` tables created through the UI. This notebook re-creates
# MAGIC bronze from the Volume CSVs to demonstrate the full ingestion flow.
# MAGIC
# MAGIC ### Setup
# MAGIC Before running, create a Volume and upload the 6 CSVs:
# MAGIC ```sql
# MAGIC CREATE VOLUME IF NOT EXISTS bronze.raw_files;
# MAGIC ```
# MAGIC Then upload CSVs to `/Volumes/workspace/bronze/raw_files/` via the Catalog UI.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE VOLUME IF NOT EXISTS bronze.raw_files;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

VOLUME_PATH = "/Volumes/workspace/bronze/raw_files"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion function

# COMMAND ----------

def ingest_csv_to_bronze(filename, table_name, encoding="UTF-8"):
    """Read a CSV from the Volume and write to a bronze Delta table."""
    path = f"{VOLUME_PATH}/{filename}"

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", encoding)
        .csv(path)
    )

    df = df.withColumn("ingested_at", current_timestamp())

    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"bronze.{table_name}")

    count = spark.table(f"bronze.{table_name}").count()
    print(f"OK bronze.{table_name}: {count:,} rows ingested from {filename}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest all 6 source files

# COMMAND ----------

ingest_csv_to_bronze("runstream.csv",                        "runstream")
ingest_csv_to_bronze("best_clean_datetime.csv",              "runbest")
ingest_csv_to_bronze("segments_clean_datetime.csv",          "runsegment")
ingest_csv_to_bronze("runstream_segments_by_kilometers.csv", "runsplitkilometer")
ingest_csv_to_bronze("all_activities_clean_datetime.csv",    "activities")
ingest_csv_to_bronze("athletes_profiles_clean.csv",          "athlete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify row counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'runstream' AS tbl, COUNT(*) AS rows FROM bronze.runstream
# MAGIC UNION ALL SELECT 'runbest', COUNT(*) FROM bronze.runbest
# MAGIC UNION ALL SELECT 'runsegment', COUNT(*) FROM bronze.runsegment
# MAGIC UNION ALL SELECT 'runsplitkilometer', COUNT(*) FROM bronze.runsplitkilometer
# MAGIC UNION ALL SELECT 'activities', COUNT(*) FROM bronze.activities
# MAGIC UNION ALL SELECT 'athlete', COUNT(*) FROM bronze.athlete;
