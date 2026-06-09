-- Databricks notebook source
-- Final concise benchmark summary.
-- Parameters: catalog, schema, run_id.
-- Uses explicit start/end event rows only, not system query-history tables.

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

-- Small detail table: one row per measured SELECT.
SELECT
  scale_label AS table_size,
  table_rows,
  CASE
    WHEN test_group = 'substring' THEN 'substring'
    WHEN test_group = 'word' THEN 'word'
    ELSE test_group
  END AS test,
  compute_label AS compute,
  CASE
    WHEN method = 'fts_ngram_index' THEN 'ngram index'
    WHEN method = 'fts_split_index' THEN 'split index'
    WHEN method = 'classic_no_index_scan' THEN 'classic scan'
    ELSE method
  END AS method,
  pattern,
  row_limit AS limit_rows,
  repetition,
  ROUND(duration_s, 3) AS seconds
FROM fts_benchmark_measured_queries
WHERE run_id = :run_id
ORDER BY table_rows, test, compute, pattern, repetition, method;

-- Small aggregate table: table size + test + compute + method = time.
SELECT
  scale_label AS table_size,
  table_rows,
  test,
  compute_label AS compute,
  method,
  pattern,
  row_limit AS limit_rows,
  repetitions,
  avg_seconds,
  p50_seconds,
  min_seconds,
  max_seconds
FROM fts_benchmark_time_summary
WHERE run_id = :run_id
ORDER BY table_rows, test, compute, method;

-- Compact comparison: indexed search time vs classic scan time.
SELECT
  scale_label AS table_size,
  table_rows,
  test,
  compute_label AS compute,
  pattern,
  row_limit AS limit_rows,
  index_avg_seconds,
  classic_avg_seconds,
  index_speedup_x
FROM fts_benchmark_speedup_summary
WHERE run_id = :run_id
ORDER BY table_rows, test, compute;
