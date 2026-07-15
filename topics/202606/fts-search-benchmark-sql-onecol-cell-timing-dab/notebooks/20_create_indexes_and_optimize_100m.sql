-- Databricks notebook source
-- Create one-column FTS indexes and run OPTIMIZE.
-- Parameters: catalog, schema, run_id.
    -- The schema itself is managed as a DAB resource in schemas.yml.
-- Important: each index is created on exactly one column: message.
-- ngram uses default ngram parameters. split uses default split parameters.

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);


-- Substring test index: ngram tokenizer, default ngram_size.
CREATE SEARCH INDEX ftsbench_si_100m_message_ngram_v1
ON fts_text_100m_ngram (message)
OPTIONS (tokenizer = 'ngram');

-- Word test index: split tokenizer, default min_token_length.
CREATE SEARCH INDEX ftsbench_si_100m_message_split_v1
ON fts_text_100m_split (message)
OPTIONS (tokenizer = 'split');

-- Requested table compaction step. This intentionally runs after index creation.
-- Because OPTIMIZE rewrites files, the indexes are refreshed FULL afterwards.
OPTIMIZE fts_text_100m_scan;
OPTIMIZE fts_text_100m_ngram;
OPTIMIZE fts_text_100m_split;

REFRESH INDEX ftsbench_si_100m_message_ngram_v1 FULL;
REFRESH INDEX ftsbench_si_100m_message_split_v1 FULL;

ANALYZE TABLE fts_text_100m_scan COMPUTE STATISTICS;
ANALYZE TABLE fts_text_100m_ngram COMPUTE STATISTICS;
ANALYZE TABLE fts_text_100m_split COMPUTE STATISTICS;

INSERT INTO fts_benchmark_object_checks
SELECT
  :run_id,
  current_timestamp(),
  '100m',
  table_name,
  'table',
  'user_column_count',
  CAST(COUNT(*) AS STRING),
  '1',
  CASE WHEN COUNT(*) = 1 AND MIN(column_name) = 'message' THEN 'PASS' ELSE 'FAIL' END
FROM information_schema.columns
WHERE table_catalog = :catalog
  AND table_schema = :schema
  AND table_name IN ('fts_text_100m_scan','fts_text_100m_ngram','fts_text_100m_split')
GROUP BY table_name;

DESCRIBE INDEX ftsbench_si_100m_message_ngram_v1;
DESCRIBE INDEX ftsbench_si_100m_message_split_v1;

SELECT *
FROM fts_benchmark_object_checks
WHERE run_id = :run_id AND scale_label = '100m'
ORDER BY event_ts, object_name, check_name;