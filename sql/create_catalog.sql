-- Run this in Databricks SQL editor or a notebook (%sql magic) to set up
-- the Unity Catalog structure for the StravaSquad lakehouse.

CREATE CATALOG IF NOT EXISTS stravasquad;

USE CATALOG stravasquad;

CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Raw CSV ingestion — append-only, with ingested_at audit column';

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Cleaned and normalized — deduplicated, dropped raw columns, enforced types';

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Business-ready — calendar dims, distance labels, activity locations, predictions';
