-- Databricks notebook source
-- SQL-only benchmark setup.
-- Parameters: catalog, schema, run_id.
-- The schema itself is managed as a DAB resource in schemas.yml.
-- Timing is captured with explicit start/end event rows around each SELECT query.

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

CREATE TABLE IF NOT EXISTS fts_benchmark_runs (
  run_id STRING,
  run_started_at TIMESTAMP,
  catalog_name STRING,
  schema_name STRING,
  notes STRING
)
USING DELTA;

-- One event row is inserted before the measured SELECT and one event row after it.
-- There are no MERGE/UPDATE statements in the benchmark timing path.
CREATE TABLE IF NOT EXISTS fts_benchmark_events (
  run_id STRING,
  compute_label STRING,
  scale_label STRING,
  table_rows BIGINT,
  test_group STRING,
  method STRING,
  pattern_label STRING,
  pattern STRING,
  row_limit INT,
  target_table STRING,
  repetition INT,
  benchmark_tag STRING,
  event_type STRING,
  event_ts TIMESTAMP,
  notes STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS fts_benchmark_object_checks (
  run_id STRING,
  event_ts TIMESTAMP,
  scale_label STRING,
  object_name STRING,
  object_type STRING,
  check_name STRING,
  observed_value STRING,
  expected_value STRING,
  status STRING
)
USING DELTA;

CREATE OR REPLACE VIEW fts_benchmark_measured_queries AS
WITH paired AS (
  SELECT
    run_id,
    compute_label,
    scale_label,
    table_rows,
    test_group,
    method,
    pattern_label,
    pattern,
    row_limit,
    target_table,
    repetition,
    benchmark_tag,
    MIN(CASE WHEN event_type = 'start' THEN event_ts END) AS test_start,
    MAX(CASE WHEN event_type = 'end' THEN event_ts END) AS test_end
  FROM fts_benchmark_events
  WHERE event_type IN ('start', 'end')
  GROUP BY
    run_id, compute_label, scale_label, table_rows, test_group, method,
    pattern_label, pattern, row_limit, target_table, repetition, benchmark_tag
)
SELECT
  *,
  timestampdiff(MILLISECOND, test_start, test_end) AS duration_ms,
  CAST(timestampdiff(MILLISECOND, test_start, test_end) AS DOUBLE) / 1000.0 AS duration_s
FROM paired
WHERE test_start IS NOT NULL
  AND test_end IS NOT NULL;

CREATE OR REPLACE VIEW fts_benchmark_time_summary AS
SELECT
  run_id,
  scale_label,
  table_rows,
  CASE
    WHEN test_group = 'substring' THEN 'substring: ngram index vs classic scan'
    WHEN test_group = 'word' THEN 'word: split index vs classic scan'
    ELSE test_group
  END AS test,
  compute_label,
  CASE
    WHEN method = 'fts_ngram_index' THEN 'ngram index'
    WHEN method = 'fts_split_index' THEN 'split index'
    WHEN method = 'classic_no_index_scan' THEN 'classic scan'
    ELSE method
  END AS method,
  pattern_label,
  pattern,
  row_limit,
  COUNT(*) AS repetitions,
  ROUND(AVG(duration_s), 3) AS avg_seconds,
  ROUND(percentile_approx(duration_s, 0.5), 3) AS p50_seconds,
  ROUND(MIN(duration_s), 3) AS min_seconds,
  ROUND(MAX(duration_s), 3) AS max_seconds
FROM fts_benchmark_measured_queries
GROUP BY
  run_id,
  scale_label,
  table_rows,
  test_group,
  compute_label,
  method,
  pattern_label,
  pattern,
  row_limit;

CREATE OR REPLACE VIEW fts_benchmark_speedup_summary AS
WITH method_times AS (
  SELECT
    run_id,
    scale_label,
    table_rows,
    test_group,
    compute_label,
    pattern_label,
    pattern,
    row_limit,
    method,
    AVG(duration_s) AS avg_seconds
  FROM fts_benchmark_measured_queries
  GROUP BY
    run_id,
    scale_label,
    table_rows,
    test_group,
    compute_label,
    pattern_label,
    pattern,
    row_limit,
    method
), pivoted AS (
  SELECT
    run_id,
    scale_label,
    table_rows,
    CASE
      WHEN test_group = 'substring' THEN 'substring'
      WHEN test_group = 'word' THEN 'word'
      ELSE test_group
    END AS test,
    compute_label,
    pattern_label,
    pattern,
    row_limit,
    MAX(CASE WHEN method = 'classic_no_index_scan' THEN avg_seconds END) AS classic_avg_seconds,
    MAX(CASE WHEN method IN ('fts_ngram_index', 'fts_split_index') THEN avg_seconds END) AS index_avg_seconds
  FROM method_times
  GROUP BY
    run_id,
    scale_label,
    table_rows,
    test_group,
    compute_label,
    pattern_label,
    pattern,
    row_limit
)
SELECT
  run_id,
  scale_label,
  table_rows,
  test,
  compute_label,
  pattern_label,
  pattern,
  row_limit,
  ROUND(index_avg_seconds, 3) AS index_avg_seconds,
  ROUND(classic_avg_seconds, 3) AS classic_avg_seconds,
  ROUND(classic_avg_seconds / NULLIF(index_avg_seconds, 0), 2) AS index_speedup_x
FROM pivoted;

-- Kept for compatibility with earlier package names.
CREATE OR REPLACE VIEW fts_benchmark_latest_summary AS
SELECT * FROM fts_benchmark_time_summary;

INSERT INTO fts_benchmark_runs
SELECT
  :run_id AS run_id,
  current_timestamp() AS run_started_at,
  :catalog AS catalog_name,
  :schema AS schema_name,
  'SQL-only one-column FTS benchmark. Timing is explicit start/end event rows around SELECT queries; no system query-history timing.' AS notes;

SELECT
  run_id,
  run_started_at,
  catalog_name,
  schema_name
FROM fts_benchmark_runs
WHERE run_id = :run_id;
