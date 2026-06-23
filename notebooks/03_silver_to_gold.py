# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver to Gold (Dimensions + Derived Tables)
# MAGIC
# MAGIC Builds gold-layer Delta tables in PySpark:
# MAGIC - `dim_date` — calendar spine from Activities date range
# MAGIC - `dim_distance_label` — 13 ordered race-distance labels
# MAGIC - `gold_activity_start_location` — first GPS point per activity

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_date — Calendar Spine

# COMMAND ----------

date_bounds = spark.table("silver.activities").select(
    F.date_sub(F.min("date"), 7).alias("min_date"),
    F.date_add(F.max("date"), 7).alias("max_date"),
).first()

df_dates = spark.sql(f"""
    SELECT explode(sequence(
        DATE '{date_bounds.min_date}',
        DATE '{date_bounds.max_date}',
        INTERVAL 1 DAY
    )) AS date
""")

df_dim_date = df_dates.select(
    F.col("date"),
    F.year("date").alias("iso_year"),
    F.weekofyear("date").alias("iso_week"),
    F.date_format("date", "EEEE").alias("day_name"),
    F.dayofweek("date").alias("day_of_week_spark"),
    F.date_trunc("week", "date").cast("date").alias("week_start_date"),
).withColumn(
    "day_of_week",
    F.when(F.col("day_of_week_spark") == 1, 7)
     .otherwise(F.col("day_of_week_spark") - 1)
).withColumn(
    "week_relative_to_today",
    (F.datediff(
        F.date_trunc("week", F.current_date()),
        F.col("week_start_date")
    ) / 7).cast("int")
).withColumn(
    "days_ago",
    F.datediff(F.current_date(), F.col("date")).cast("int")
).drop("day_of_week_spark")

df_dim_date.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.dim_date")

print(f"gold.dim_date: {df_dim_date.count()} rows ({date_bounds.min_date} to {date_bounds.max_date})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_distance_label — 13 Race Distances

# COMMAND ----------

distance_data = [
    (1,  "400m",           400.0),
    (2,  "1/2 mile",       804.672),
    (3,  "1K",             1000.0),
    (4,  "1 mile",         1609.344),
    (5,  "2 mile",         3218.688),
    (6,  "5K",             5000.0),
    (7,  "10K",            10000.0),
    (8,  "15K",            15000.0),
    (9,  "10 mile",        16093.44),
    (10, "20K",            20000.0),
    (11, "Half-Marathon",  21097.5),
    (12, "30K",            30000.0),
    (13, "Marathon",       42195.0),
]

schema = StructType([
    StructField("sort_order", IntegerType(), False),
    StructField("distance_label", StringType(), False),
    StructField("standard_distance_m", DoubleType(), False),
])

df_dist = spark.createDataFrame(distance_data, schema)

df_dist.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.dim_distance_label")

print(f"gold.dim_distance_label: {df_dist.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_activity_start_location — First GPS Point per Activity

# COMMAND ----------

df_first_point = (
    spark.table("silver.runstream")
    .filter("lat IS NOT NULL AND lon IS NOT NULL")
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("activity_id").orderBy("time_s")
    ))
    .filter("rn = 1")
    .select("activity_id", F.col("lat").alias("start_lat"), F.col("lon").alias("start_lon"))
)

df_gold_loc = (
    spark.table("silver.activities").alias("a")
    .join(df_first_point.alias("fp"), "activity_id", "inner")
    .select(
        "a.activity_id", "a.athlete_id", "a.athlete_name",
        "a.date", "a.activity_type",
        "fp.start_lat", "fp.start_lon",
    )
)

df_gold_loc.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.gold_activity_start_location")

print(f"gold.gold_activity_start_location: {df_gold_loc.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

for table in ["dim_date", "dim_distance_label", "gold_activity_start_location"]:
    count = spark.table(f"gold.{table}").count()
    print(f"  gold.{table}: {count:,} rows")
