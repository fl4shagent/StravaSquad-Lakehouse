# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest CSVs to Bronze (Delta Lake)
# MAGIC
# MAGIC **Note:** For the initial load, CSVs were uploaded directly through the
# MAGIC Databricks UI into `bronze.*` tables. This notebook is the template for
# MAGIC future automated ingestion from S3 when the bucket connection is configured.
# MAGIC
# MAGIC To verify the bronze tables are loaded:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'runstream' AS tbl, COUNT(*) AS rows FROM bronze.runstream
# MAGIC UNION ALL SELECT 'activities', COUNT(*) FROM bronze.activities
# MAGIC UNION ALL SELECT 'athlete', COUNT(*) FROM bronze.athlete
# MAGIC UNION ALL SELECT 'runbest', COUNT(*) FROM bronze.runbest
# MAGIC UNION ALL SELECT 'runsegment', COUNT(*) FROM bronze.runsegment
# MAGIC UNION ALL SELECT 'runsplitkilometer', COUNT(*) FROM bronze.runsplitkilometer;
