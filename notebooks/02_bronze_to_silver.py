# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze to Silver (Clean, Dedupe, Normalize)
# MAGIC
# MAGIC Reads bronze Delta tables, applies the same normalization rules as the
# MAGIC SQL Server `silver.*` schema:
# MAGIC - Deduplicates `RunStream` on `(activity_id, time_s)` — fixes the 635K
# MAGIC   duplicate incident from the original pipeline
# MAGIC - Drops unnecessary columns per table
# MAGIC - Uses Delta Lake MERGE for upsert-strategy tables (Activities, Athlete)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## RunStream — deduplicate on (activity_id, time_s)

# COMMAND ----------

df = spark.table("bronze.runstream")

w = Window.partitionBy("activity_id", "time_s").orderBy(F.lit(1))
df_silver = (
    df.withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
)

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.runstream")

print(f"silver.runstream: {df_silver.count():,} rows (bronze had {df.count():,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## RunBest — drop start_index, end_index

# COMMAND ----------

df = spark.table("bronze.runbest")
drop_cols = [c for c in ["start_index", "end_index"] if c in df.columns]
df_silver = df.drop(*drop_cols).dropDuplicates(["id"])

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.runbest")

print(f"silver.runbest: {df_silver.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## RunSegment — drop device_watts, hidden, visibility, kom_rank

# COMMAND ----------

df = spark.table("bronze.runsegment")
drop_cols = [c for c in ["device_watts", "hidden", "visibility", "kom_rank"] if c in df.columns]
df_silver = df.drop(*drop_cols).dropDuplicates(["id"])

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.runsegment")

print(f"silver.runsegment: {df_silver.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## RunSplitKilometer — deduplicate on (activity_id, segment_number)

# COMMAND ----------

df = spark.table("bronze.runsplitkilometer")
df_silver = df.dropDuplicates(["activity_id", "segment_number"])

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.runsplitkilometer")

print(f"silver.runsplitkilometer: {df_silver.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Activities — MERGE (upsert on activity_id)

# COMMAND ----------

df_new = spark.table("bronze.activities")

target_table = "silver.activities"
if spark.catalog.tableExists(target_table):
    silver = DeltaTable.forName(spark, target_table)
    silver.alias("t").merge(
        df_new.alias("s"),
        "t.activity_id = s.activity_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print(f"silver.activities: MERGED {df_new.count():,} rows")
else:
    df_new.write.format("delta").saveAsTable(target_table)
    print(f"silver.activities: created with {df_new.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Athlete — MERGE (upsert on athlete_id), drop 'id' column

# COMMAND ----------

df_new = spark.table("bronze.athlete")
if "id" in df_new.columns:
    df_new = df_new.drop("id")

target_table = "silver.athlete"
if spark.catalog.tableExists(target_table):
    silver = DeltaTable.forName(spark, target_table)
    silver.alias("t").merge(
        df_new.alias("s"),
        "t.athlete_id = s.athlete_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print(f"silver.athlete: MERGED {df_new.count():,} rows")
else:
    df_new.write.format("delta").saveAsTable(target_table)
    print(f"silver.athlete: created with {df_new.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

for table in ["runstream", "runbest", "runsegment", "runsplitkilometer", "activities", "athlete"]:
    count = spark.table(f"silver.{table}").count()
    print(f"  silver.{table}: {count:,} rows")
