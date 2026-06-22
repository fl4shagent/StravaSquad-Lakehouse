# StravaSquad Lakehouse

![Lint](https://github.com/fl4shagent/StravaSquad-Lakehouse/actions/workflows/lint.yml/badge.svg)

A **Databricks lakehouse** implementation of the [StravaSquad](https://github.com/fl4shagent/StravaSquad) multi-runner analytics pipeline. Migrates the local SQL Server medallion architecture to **Delta Lake on AWS**, orchestrated by **Databricks Workflows** with **Unity Catalog** governance.

**Scale:** 10 athletes · ~2M GPS points · bronze/silver/gold Delta tables

---

## Architecture

```
Strava API → Python scripts → CSV files → S3 bucket
                                              │
                                    ┌─────────▼──────────┐
                                    │  Databricks (AWS)   │
                                    │                     │
                                    │  ┌───────────────┐  │
                                    │  │ Bronze (Delta) │  │  ← Raw CSV append
                                    │  └───────┬───────┘  │
                                    │          │          │
                                    │  ┌───────▼───────┐  │
                                    │  │ Silver (Delta) │  │  ← Deduped, normalized
                                    │  └───────┬───────┘  │
                                    │          │          │
                                    │  ┌───────▼───────┐  │
                                    │  │  Gold (Delta)  │  │  ← Dims + predictions
                                    │  └───────────────┘  │
                                    │                     │
                                    │  Unity Catalog      │  ← Governance + lineage
                                    │  Databricks Workflow │  ← Orchestration
                                    └─────────────────────┘
```

---

## Pipeline (Databricks Workflow)

```
01_ingest_to_bronze → 02_bronze_to_silver → 03_silver_to_gold → 04_predict_pb → 05_data_quality_tests
```

| Notebook | What it does | Key technique |
|---|---|---|
| `01_ingest_to_bronze` | Read CSVs from S3, append to Delta tables | `spark.read.csv()` + `ingested_at` audit column |
| `02_bronze_to_silver` | Clean, dedupe, normalize | `Window` + `row_number()` for dedup, `DeltaTable.merge()` for upserts |
| `03_silver_to_gold` | Build dimensions + derived tables | `sequence()` + `explode()` for date spine, window functions |
| `04_predict_pb` | Race-time predictions (3 formulas) | Riegel / VDOT / elevation-adjusted, backtested vs real HM |
| `05_data_quality_tests` | 17 automated quality assertions | Uniqueness, not-null, accepted values, composite keys |

---

## Delta Lake Features Used

| Feature | Where | Why |
|---|---|---|
| **Append mode** | Bronze ingestion | Preserve raw data history, audit trail via `ingested_at` |
| **MERGE (upsert)** | Silver Activities + Athlete | Handle both new and updated rows without duplicates |
| **Overwrite with schema evolution** | Silver fact tables, Gold | Clean rebuild with `overwriteSchema` for schema changes |
| **Time travel** | All tables | `DESCRIBE HISTORY` shows versioned writes — audit/rollback capability |

---

## Data Quality (17 Tests)

Same assertions as the SQL Server dbt tests, implemented as PySpark SQL queries:

- `dim_date.date` — unique, not null
- `dim_distance_label.distance_label` + `sort_order` — unique, not null
- `gold_activity_start_location.activity_id` — unique, not null
- `pb_prediction` — not null on keys, composite uniqueness
- `race_forecast` — not null on keys, `method` accepted values, composite uniqueness

---

## Comparison: SQL Server vs. Databricks

| Aspect | StravaSquad (SQL Server) | StravaSquad-Lakehouse (Databricks) |
|---|---|---|
| Storage | Local SQL Server tables | Delta Lake on S3 |
| Compute | pandas + pyodbc | PySpark |
| Gold layer | dbt-sqlserver (17 tests) | PySpark notebooks (17 tests) |
| Orchestration | GitHub Actions CI | Databricks Workflows |
| Governance | None | Unity Catalog (catalog → schema → table lineage) |
| Dedup mechanism | SQL `ROW_NUMBER() OVER` | PySpark `Window` + `row_number()` |
| Upsert mechanism | `DELETE` + `INSERT` | Delta Lake `MERGE` |
| Time travel | Not available | Built-in (`DESCRIBE HISTORY`, `VERSION AS OF`) |

---

## Setup Guide

### Prerequisites
- AWS account with S3 access
- Databricks workspace on AWS (14-day free trial available)

### Steps

1. **Create S3 bucket**: `stravasquad-lakehouse` with folder `raw/`

2. **Upload source CSVs** to `s3://stravasquad-lakehouse/raw/`:
   ```bash
   aws s3 cp runstream.csv s3://stravasquad-lakehouse/raw/
   aws s3 cp best_clean_datetime.csv s3://stravasquad-lakehouse/raw/
   aws s3 cp segments_clean_datetime.csv s3://stravasquad-lakehouse/raw/
   aws s3 cp runstream_segments_by_kilometers.csv s3://stravasquad-lakehouse/raw/
   aws s3 cp all_activities_clean_datetime.csv s3://stravasquad-lakehouse/raw/
   aws s3 cp athletes_profiles_clean.csv s3://stravasquad-lakehouse/raw/
   ```

3. **Create Unity Catalog** — run `sql/create_catalog.sql` in a Databricks SQL editor

4. **Connect repo** — Add this GitHub repo to Databricks Repos
   (`Workspace → Repos → Add Repo → paste GitHub URL`)

5. **Create Workflow** — import `workflows/strava_pipeline.json` via Databricks Jobs API
   or manually create tasks pointing to each notebook

6. **Run the workflow** — click "Run Now" and watch the DAG execute

---

## Tech Stack

| Tool | Role |
|---|---|
| Databricks (AWS) | Compute + orchestration |
| Delta Lake | ACID storage format (bronze/silver/gold) |
| PySpark | Data processing |
| Unity Catalog | Data governance + lineage |
| S3 | Raw file landing zone |
| Python (NumPy, SciPy) | Prediction formulas |
| GitHub Actions | Notebook linting CI |

---

## Related

- [StravaSquad](https://github.com/fl4shagent/StravaSquad) — the original SQL Server version with dbt, Streamlit dashboard, and full GitHub Actions CI (dbt seed + run + test against a SQL Server container)
