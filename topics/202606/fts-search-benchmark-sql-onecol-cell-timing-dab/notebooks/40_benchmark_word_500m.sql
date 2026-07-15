-- Databricks notebook source
-- WORD benchmark with explicit cell-level timing.
-- Parameters: catalog, schema, run_id, compute_label.
-- Each measured test is three separate notebook cells:
--   1) INSERT start timestamp
--   2) SELECT ... WHERE search(message, same_pattern) LIMIT 1
--   3) INSERT end timestamp
-- No MERGE, UPDATE, or system.query.history timing is used.

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

-- COMMAND ----------

-- START: FTSBENCH|scale=500m|group=word|method=fts_split_index|pattern=tail_word_limit_1|limit=1|rep=1
INSERT INTO fts_benchmark_events
SELECT
  :run_id AS run_id,
  :compute_label AS compute_label,
  '500m' AS scale_label,
  500000000L AS table_rows,
  'word' AS test_group,
  'fts_split_index' AS method,
  'tail_word_limit_1' AS pattern_label,
  'zztailwordprobe' AS pattern,
  1 AS row_limit,
  'fts_text_500m_split' AS target_table,
  1 AS repetition,
  'FTSBENCH|scale=500m|group=word|method=fts_split_index|pattern=tail_word_limit_1|limit=1|rep=1' AS benchmark_tag,
  'start' AS event_type,
  current_timestamp() AS event_ts,
  'split index table; word mode; tail word probe; limit 1' AS notes;
-- COMMAND ----------

-- MEASURED QUERY: FTSBENCH|scale=500m|group=word|method=fts_split_index|pattern=tail_word_limit_1|limit=1|rep=1
-- The predicate and LIMIT are intentionally identical for index and scan methods.
SELECT message
FROM fts_text_500m_split
WHERE search(message, 'zztailwordprobe', mode => 'word')
LIMIT 1;
-- COMMAND ----------

-- END: FTSBENCH|scale=500m|group=word|method=fts_split_index|pattern=tail_word_limit_1|limit=1|rep=1
INSERT INTO fts_benchmark_events
SELECT
  :run_id AS run_id,
  :compute_label AS compute_label,
  '500m' AS scale_label,
  500000000L AS table_rows,
  'word' AS test_group,
  'fts_split_index' AS method,
  'tail_word_limit_1' AS pattern_label,
  'zztailwordprobe' AS pattern,
  1 AS row_limit,
  'fts_text_500m_split' AS target_table,
  1 AS repetition,
  'FTSBENCH|scale=500m|group=word|method=fts_split_index|pattern=tail_word_limit_1|limit=1|rep=1' AS benchmark_tag,
  'end' AS event_type,
  current_timestamp() AS event_ts,
  'split index table; word mode; tail word probe; limit 1' AS notes;
-- COMMAND ----------

-- START: FTSBENCH|scale=500m|group=word|method=classic_no_index_scan|pattern=tail_word_limit_1|limit=1|rep=1
INSERT INTO fts_benchmark_events
SELECT
  :run_id AS run_id,
  :compute_label AS compute_label,
  '500m' AS scale_label,
  500000000L AS table_rows,
  'word' AS test_group,
  'classic_no_index_scan' AS method,
  'tail_word_limit_1' AS pattern_label,
  'zztailwordprobe' AS pattern,
  1 AS row_limit,
  'fts_text_500m_scan' AS target_table,
  1 AS repetition,
  'FTSBENCH|scale=500m|group=word|method=classic_no_index_scan|pattern=tail_word_limit_1|limit=1|rep=1' AS benchmark_tag,
  'start' AS event_type,
  current_timestamp() AS event_ts,
  'no index scan table; word mode; tail word probe; limit 1' AS notes;
-- COMMAND ----------

-- MEASURED QUERY: FTSBENCH|scale=500m|group=word|method=classic_no_index_scan|pattern=tail_word_limit_1|limit=1|rep=1
-- The predicate and LIMIT are intentionally identical for index and scan methods.
SELECT message
FROM fts_text_500m_scan
WHERE search(message, 'zztailwordprobe', mode => 'word')
LIMIT 1;
-- COMMAND ----------

-- END: FTSBENCH|scale=500m|group=word|method=classic_no_index_scan|pattern=tail_word_limit_1|limit=1|rep=1
INSERT INTO fts_benchmark_events
SELECT
  :run_id AS run_id,
  :compute_label AS compute_label,
  '500m' AS scale_label,
  500000000L AS table_rows,
  'word' AS test_group,
  'classic_no_index_scan' AS method,
  'tail_word_limit_1' AS pattern_label,
  'zztailwordprobe' AS pattern,
  1 AS row_limit,
  'fts_text_500m_scan' AS target_table,
  1 AS repetition,
  'FTSBENCH|scale=500m|group=word|method=classic_no_index_scan|pattern=tail_word_limit_1|limit=1|rep=1' AS benchmark_tag,
  'end' AS event_type,
  current_timestamp() AS event_ts,
  'no index scan table; word mode; tail word probe; limit 1' AS notes;
-- COMMAND ----------

-- START: FTSBENCH|scale=500m|group=word|method=fts_split_index|pattern=tail_word_limit_1|limit=1|rep=2
INSERT INTO fts_benchmark_events
SELECT
  :run_id AS run_id,
  :compute_label AS compute_label,
  '500m' AS scale_label,
  500000000L AS table_rows,
  'word' AS test_group,
  'fts_split_index' AS method,
  'tail_word_limit_1' AS pattern_label,
  'zztailwordprobe' AS pattern,
  1 AS row_limit,
  'fts_text_500m_split' AS target_table,
  2 AS repetition,
  'FTSBENCH|scale=500m|group=word|method=fts_split_index|pattern=tail_word_limit_1|limit=1|rep=2' AS benchmark_tag,
  'start' AS event_type,
  current_timestamp() AS event_ts,
  'split index table; word mode; tail word probe; limit 1' AS notes;
-- COMMAND ----------

-- MEASURED QUERY: FTSBENCH|scale=500m|group=word|method=fts_split_index|pattern=tail_word_limit_1|limit=1|rep=2
-- The predicate and LIMIT are intentionally identical for index and scan methods.
SELECT message
FROM fts_text_500m_split
WHERE search(message, 'zztailwordprobe', mode => 'word')
LIMIT 1;
-- COMMAND ----------

-- END: FTSBENCH|scale=500m|group=word|method=fts_split_index|pattern=tail_word_limit_1|limit=1|rep=2
INSERT INTO fts_benchmark_events
SELECT
  :run_id AS run_id,
  :compute_label AS compute_label,
  '500m' AS scale_label,
  500000000L AS table_rows,
  'word' AS test_group,
  'fts_split_index' AS method,
  'tail_word_limit_1' AS pattern_label,
  'zztailwordprobe' AS pattern,
  1 AS row_limit,
  'fts_text_500m_split' AS target_table,
  2 AS repetition,
  'FTSBENCH|scale=500m|group=word|method=fts_split_index|pattern=tail_word_limit_1|limit=1|rep=2' AS benchmark_tag,
  'end' AS event_type,
  current_timestamp() AS event_ts,
  'split index table; word mode; tail word probe; limit 1' AS notes;
-- COMMAND ----------

-- START: FTSBENCH|scale=500m|group=word|method=classic_no_index_scan|pattern=tail_word_limit_1|limit=1|rep=2
INSERT INTO fts_benchmark_events
SELECT
  :run_id AS run_id,
  :compute_label AS compute_label,
  '500m' AS scale_label,
  500000000L AS table_rows,
  'word' AS test_group,
  'classic_no_index_scan' AS method,
  'tail_word_limit_1' AS pattern_label,
  'zztailwordprobe' AS pattern,
  1 AS row_limit,
  'fts_text_500m_scan' AS target_table,
  2 AS repetition,
  'FTSBENCH|scale=500m|group=word|method=classic_no_index_scan|pattern=tail_word_limit_1|limit=1|rep=2' AS benchmark_tag,
  'start' AS event_type,
  current_timestamp() AS event_ts,
  'no index scan table; word mode; tail word probe; limit 1' AS notes;
-- COMMAND ----------

-- MEASURED QUERY: FTSBENCH|scale=500m|group=word|method=classic_no_index_scan|pattern=tail_word_limit_1|limit=1|rep=2
-- The predicate and LIMIT are intentionally identical for index and scan methods.
SELECT message
FROM fts_text_500m_scan
WHERE search(message, 'zztailwordprobe', mode => 'word')
LIMIT 1;
-- COMMAND ----------

-- END: FTSBENCH|scale=500m|group=word|method=classic_no_index_scan|pattern=tail_word_limit_1|limit=1|rep=2
INSERT INTO fts_benchmark_events
SELECT
  :run_id AS run_id,
  :compute_label AS compute_label,
  '500m' AS scale_label,
  500000000L AS table_rows,
  'word' AS test_group,
  'classic_no_index_scan' AS method,
  'tail_word_limit_1' AS pattern_label,
  'zztailwordprobe' AS pattern,
  1 AS row_limit,
  'fts_text_500m_scan' AS target_table,
  2 AS repetition,
  'FTSBENCH|scale=500m|group=word|method=classic_no_index_scan|pattern=tail_word_limit_1|limit=1|rep=2' AS benchmark_tag,
  'end' AS event_type,
  current_timestamp() AS event_ts,
  'no index scan table; word mode; tail word probe; limit 1' AS notes;
-- COMMAND ----------

-- START: FTSBENCH|scale=500m|group=word|method=fts_split_index|pattern=tail_word_limit_1|limit=1|rep=3
INSERT INTO fts_benchmark_events
SELECT
  :run_id AS run_id,
  :compute_label AS compute_label,
  '500m' AS scale_label,
  500000000L AS table_rows,
  'word' AS test_group,
  'fts_split_index' AS method,
  'tail_word_limit_1' AS pattern_label,
  'zztailwordprobe' AS pattern,
  1 AS row_limit,
  'fts_text_500m_split' AS target_table,
  3 AS repetition,
  'FTSBENCH|scale=500m|group=word|method=fts_split_index|pattern=tail_word_limit_1|limit=1|rep=3' AS benchmark_tag,
  'start' AS event_type,
  current_timestamp() AS event_ts,
  'split index table; word mode; tail word probe; limit 1' AS notes;
-- COMMAND ----------

-- MEASURED QUERY: FTSBENCH|scale=500m|group=word|method=fts_split_index|pattern=tail_word_limit_1|limit=1|rep=3
-- The predicate and LIMIT are intentionally identical for index and scan methods.
SELECT message
FROM fts_text_500m_split
WHERE search(message, 'zztailwordprobe', mode => 'word')
LIMIT 1;
-- COMMAND ----------

-- END: FTSBENCH|scale=500m|group=word|method=fts_split_index|pattern=tail_word_limit_1|limit=1|rep=3
INSERT INTO fts_benchmark_events
SELECT
  :run_id AS run_id,
  :compute_label AS compute_label,
  '500m' AS scale_label,
  500000000L AS table_rows,
  'word' AS test_group,
  'fts_split_index' AS method,
  'tail_word_limit_1' AS pattern_label,
  'zztailwordprobe' AS pattern,
  1 AS row_limit,
  'fts_text_500m_split' AS target_table,
  3 AS repetition,
  'FTSBENCH|scale=500m|group=word|method=fts_split_index|pattern=tail_word_limit_1|limit=1|rep=3' AS benchmark_tag,
  'end' AS event_type,
  current_timestamp() AS event_ts,
  'split index table; word mode; tail word probe; limit 1' AS notes;
-- COMMAND ----------

-- START: FTSBENCH|scale=500m|group=word|method=classic_no_index_scan|pattern=tail_word_limit_1|limit=1|rep=3
INSERT INTO fts_benchmark_events
SELECT
  :run_id AS run_id,
  :compute_label AS compute_label,
  '500m' AS scale_label,
  500000000L AS table_rows,
  'word' AS test_group,
  'classic_no_index_scan' AS method,
  'tail_word_limit_1' AS pattern_label,
  'zztailwordprobe' AS pattern,
  1 AS row_limit,
  'fts_text_500m_scan' AS target_table,
  3 AS repetition,
  'FTSBENCH|scale=500m|group=word|method=classic_no_index_scan|pattern=tail_word_limit_1|limit=1|rep=3' AS benchmark_tag,
  'start' AS event_type,
  current_timestamp() AS event_ts,
  'no index scan table; word mode; tail word probe; limit 1' AS notes;
-- COMMAND ----------

-- MEASURED QUERY: FTSBENCH|scale=500m|group=word|method=classic_no_index_scan|pattern=tail_word_limit_1|limit=1|rep=3
-- The predicate and LIMIT are intentionally identical for index and scan methods.
SELECT message
FROM fts_text_500m_scan
WHERE search(message, 'zztailwordprobe', mode => 'word')
LIMIT 1;
-- COMMAND ----------

-- END: FTSBENCH|scale=500m|group=word|method=classic_no_index_scan|pattern=tail_word_limit_1|limit=1|rep=3
INSERT INTO fts_benchmark_events
SELECT
  :run_id AS run_id,
  :compute_label AS compute_label,
  '500m' AS scale_label,
  500000000L AS table_rows,
  'word' AS test_group,
  'classic_no_index_scan' AS method,
  'tail_word_limit_1' AS pattern_label,
  'zztailwordprobe' AS pattern,
  1 AS row_limit,
  'fts_text_500m_scan' AS target_table,
  3 AS repetition,
  'FTSBENCH|scale=500m|group=word|method=classic_no_index_scan|pattern=tail_word_limit_1|limit=1|rep=3' AS benchmark_tag,
  'end' AS event_type,
  current_timestamp() AS event_ts,
  'no index scan table; word mode; tail word probe; limit 1' AS notes;