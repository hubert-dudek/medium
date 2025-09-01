-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC     **cache_origin_statement_id:** For query results fetched from cache, this field contains the statement ID of the query that originally inserted the result into the cache.
-- MAGIC     query_parameters: A struct containing named and positional parameters used in parameterized queries.
-- MAGIC     written_rows: The number of rows of persistent data written to cloud object storage.
-- MAGIC     written_files: Number of files of persistent data written to cloud object storage.
-- MAGIC

-- COMMAND ----------

SELECT
  statement_text,
  statement_id,
  written_rows,
  written_files,
  query_parameters,
  cache_origin_statement_id
FROM
  system.query.history
WHERE
  statement_id <> cache_origin_statement_id
ORDER BY
  start_time DESC;
