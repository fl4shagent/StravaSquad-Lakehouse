# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Tests
# MAGIC
# MAGIC Same 17 assertions as the SQL Server dbt tests, expressed as PySpark queries.
# MAGIC Run after the pipeline completes to validate data integrity.

# COMMAND ----------

CATALOG = "stravasquad"
failures = []

def assert_test(name, query, expect_zero=True):
    """Run a SQL query; pass if result count is 0 (no violations)."""
    result = spark.sql(query)
    count = result.count()
    status = "PASS" if (count == 0) == expect_zero else "FAIL"
    print(f"  [{status}] {name}" + (f" ({count} violations)" if count > 0 else ""))
    if status == "FAIL":
        failures.append(name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: dim_date

# COMMAND ----------

assert_test(
    "dim_date.date is unique",
    f"SELECT date, COUNT(*) FROM {CATALOG}.gold.dim_date GROUP BY date HAVING COUNT(*) > 1"
)
assert_test(
    "dim_date.date is not null",
    f"SELECT * FROM {CATALOG}.gold.dim_date WHERE date IS NULL"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: dim_distance_label

# COMMAND ----------

assert_test(
    "dim_distance_label.distance_label is unique",
    f"SELECT distance_label, COUNT(*) FROM {CATALOG}.gold.dim_distance_label GROUP BY distance_label HAVING COUNT(*) > 1"
)
assert_test(
    "dim_distance_label.distance_label is not null",
    f"SELECT * FROM {CATALOG}.gold.dim_distance_label WHERE distance_label IS NULL"
)
assert_test(
    "dim_distance_label.sort_order is unique",
    f"SELECT sort_order, COUNT(*) FROM {CATALOG}.gold.dim_distance_label GROUP BY sort_order HAVING COUNT(*) > 1"
)
assert_test(
    "dim_distance_label.sort_order is not null",
    f"SELECT * FROM {CATALOG}.gold.dim_distance_label WHERE sort_order IS NULL"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: gold_activity_start_location

# COMMAND ----------

assert_test(
    "gold_activity_start_location.activity_id is unique",
    f"SELECT activity_id, COUNT(*) FROM {CATALOG}.gold.gold_activity_start_location GROUP BY activity_id HAVING COUNT(*) > 1"
)
assert_test(
    "gold_activity_start_location.activity_id is not null",
    f"SELECT * FROM {CATALOG}.gold.gold_activity_start_location WHERE activity_id IS NULL"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: pb_prediction

# COMMAND ----------

assert_test(
    "pb_prediction.athlete_id is not null",
    f"SELECT * FROM {CATALOG}.gold.pb_prediction WHERE athlete_id IS NULL"
)
assert_test(
    "pb_prediction.distance_label is not null",
    f"SELECT * FROM {CATALOG}.gold.pb_prediction WHERE distance_label IS NULL"
)
assert_test(
    "pb_prediction.prediction_date is not null",
    f"SELECT * FROM {CATALOG}.gold.pb_prediction WHERE prediction_date IS NULL"
)
assert_test(
    "pb_prediction composite key is unique",
    f"""SELECT athlete_id, distance_label, prediction_date, COUNT(*)
        FROM {CATALOG}.gold.pb_prediction
        GROUP BY athlete_id, distance_label, prediction_date
        HAVING COUNT(*) > 1"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: race_forecast

# COMMAND ----------

assert_test(
    "race_forecast.athlete_id is not null",
    f"SELECT * FROM {CATALOG}.gold.race_forecast WHERE athlete_id IS NULL"
)
assert_test(
    "race_forecast.target_distance_label is not null",
    f"SELECT * FROM {CATALOG}.gold.race_forecast WHERE target_distance_label IS NULL"
)
assert_test(
    "race_forecast.as_of_date is not null",
    f"SELECT * FROM {CATALOG}.gold.race_forecast WHERE as_of_date IS NULL"
)
assert_test(
    "race_forecast.method accepted values",
    f"SELECT * FROM {CATALOG}.gold.race_forecast WHERE method NOT IN ('riegel', 'vdot', 'elevation_adjusted')"
)
assert_test(
    "race_forecast composite key is unique",
    f"""SELECT athlete_id, target_distance_label, method, as_of_date, COUNT(*)
        FROM {CATALOG}.gold.race_forecast
        GROUP BY athlete_id, target_distance_label, method, as_of_date
        HAVING COUNT(*) > 1"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total = 17
passed = total - len(failures)
print(f"\n{'='*50}")
print(f"Results: {passed}/{total} tests passed")
if failures:
    print(f"FAILED: {', '.join(failures)}")
    raise AssertionError(f"{len(failures)} data quality test(s) failed")
else:
    print("All tests passed!")
