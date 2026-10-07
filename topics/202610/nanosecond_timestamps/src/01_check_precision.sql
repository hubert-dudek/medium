-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Nanosecond timestamps: check the announcement
-- MAGIC Requires nanosecond timestamp support enabled on the selected compute.
-- MAGIC See `../README.md` for sources and the feature-gate result observed on 2026-10-07.
-- MAGIC These examples use synthetic rows and SELECT statements only.
-- MAGIC Expected results below are acceptance criteria, not recorded successful output.

-- COMMAND ----------

SELECT current_version() AS engine_version, current_timezone() AS session_timezone;

-- COMMAND ----------

-- Explicit precision is essential: the default remains six fractional digits.
-- This cell is also the availability probe. FEATURE_NOT_ENABLED means the
-- selected compute cannot yet run this demo with its current configuration.
WITH sample AS (
  SELECT '2026-10-07 09:30:00.123456789' AS raw_time
)
SELECT
  raw_time,
  typeof(CAST(raw_time AS TIMESTAMP(9))) AS ltz_type,
  CAST(CAST(raw_time AS TIMESTAMP(9)) AS STRING) AS ltz_nanos,
  typeof(CAST(raw_time AS TIMESTAMP_NTZ(9))) AS ntz_type,
  CAST(CAST(raw_time AS TIMESTAMP_NTZ(9)) AS STRING) AS ntz_nanos,
  CAST(CAST(raw_time AS TIMESTAMP_NTZ) AS STRING) AS default_micros
FROM sample;

-- COMMAND ----------

-- Input rows deliberately arrive out of order; ORDER BY uses the native type.
-- Expected order: sensor_before, trigger, sensor_after.
WITH events AS (
  SELECT event_name, CAST(raw_time AS TIMESTAMP_NTZ(9)) AS event_time
  FROM VALUES
    ('sensor_after',  '2026-10-07 09:30:00.123456909'),
    ('sensor_before', '2026-10-07 09:30:00.123456101'),
    ('trigger',       '2026-10-07 09:30:00.123456505')
  AS input(event_name, raw_time)
)
SELECT event_name, CAST(event_time AS STRING) AS event_time_nanos
FROM events
ORDER BY event_time;

-- COMMAND ----------

-- The same three events collapse to one timestamp at microsecond precision.
WITH events AS (
  SELECT
    CAST(raw_time AS TIMESTAMP_NTZ(9)) AS nanos,
    CAST(raw_time AS TIMESTAMP_NTZ(6)) AS micros
  FROM VALUES
    ('2026-10-07 09:30:00.123456909'),
    ('2026-10-07 09:30:00.123456101'),
    ('2026-10-07 09:30:00.123456505')
  AS input(raw_time)
)
SELECT
  COUNT(DISTINCT nanos) AS distinct_nanos,
  COUNT(DISTINCT micros) AS distinct_micros,
  assert_true(COUNT(DISTINCT nanos) = 3, 'Nanosecond timestamps lost distinct events') AS nanos_check,
  assert_true(COUNT(DISTINCT micros) = 1, 'Unexpected microsecond baseline') AS micros_check
FROM events;

-- COMMAND ----------

-- Check preservation as strings inside SQL, avoiding Python datetime's
-- microsecond-only representation. Successful assert_true calls return NULL.
WITH sample AS (
  SELECT '2026-10-07 09:30:00.123456789' AS raw_time
)
SELECT
  assert_true(
    CAST(CAST(raw_time AS TIMESTAMP(9)) AS STRING) = raw_time,
    'TIMESTAMP(9) lost fractional digits'
  ) AS ltz_round_trip,
  assert_true(
    CAST(CAST(raw_time AS TIMESTAMP_NTZ(9)) AS STRING) = raw_time,
    'TIMESTAMP_NTZ(9) lost fractional digits'
  ) AS ntz_round_trip,
  assert_true(
    CAST('2026-10-07 09:30:00.123456789+02:00' AS TIMESTAMP(9))
      = CAST('2026-10-07 07:30:00.123456789Z' AS TIMESTAMP(9)),
    'Equivalent instants did not compare equal'
  ) AS timezone_check
FROM sample;