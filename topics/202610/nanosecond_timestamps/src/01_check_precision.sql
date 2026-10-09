-- Databricks notebook source
WITH sample AS (
  SELECT '2026-10-09 09:30:00.123456789' AS original
)
SELECT
  original,
  CAST(CAST(original AS TIMESTAMP) AS STRING) AS microseconds,
  CAST(CAST(original AS TIMESTAMP_NTZ) AS STRING) AS microseconds_ntz
FROM sample;

-- COMMAND ----------

WITH sample AS (
  SELECT '2026-10-09 09:30:00.123456789' AS original
)
SELECT
  original,
  CAST(CAST(original AS TIMESTAMP(9)) AS STRING) AS nanoseconds,
  typeof(CAST(original AS TIMESTAMP(9))) AS data_type
FROM sample;

-- COMMAND ----------

WITH sample AS (
  SELECT '2026-10-09 09:30:00.123456789' AS original
)
SELECT
  original,
  CAST(CAST(original AS TIMESTAMP_NTZ(9)) AS STRING) AS nanoseconds_ntz,
  typeof(CAST(original AS TIMESTAMP_NTZ(9))) AS data_type
FROM sample;
