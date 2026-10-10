-- Databricks notebook source
SELECT
  'classic' AS cdf_mode,
  _change_type,
  count(*) AS change_rows,
  sum(length(payload)) AS logical_payload_bytes,
  sum(CAST(xxhash64(id, bucket, amount, revision, payload, _change_type) AS DECIMAL(38, 0))) AS hash_sum,
  sum(CAST(xxhash64(_change_type, payload, revision, amount, bucket, id) AS DECIMAL(38, 0))) AS reverse_hash_sum,
  min(id) AS minimum_id,
  max(id) AS maximum_id
FROM table_changes('main.automatic_cdf_20261010.demo_storage_classic', 2)
GROUP BY _change_type
ORDER BY _change_type;

-- COMMAND ----------

SELECT
  'automatic' AS cdf_mode,
  _change_type,
  count(*) AS change_rows,
  sum(length(payload)) AS logical_payload_bytes,
  sum(CAST(xxhash64(id, bucket, amount, revision, payload, _change_type) AS DECIMAL(38, 0))) AS hash_sum,
  sum(CAST(xxhash64(_change_type, payload, revision, amount, bucket, id) AS DECIMAL(38, 0))) AS reverse_hash_sum,
  min(id) AS minimum_id,
  max(id) AS maximum_id
FROM table_changes('main.automatic_cdf_20261010.demo_storage_automatic', 2)
GROUP BY _change_type
ORDER BY _change_type;

-- COMMAND ----------

SELECT
  'automatic' AS cdf_mode,
  _change_type,
  count(*) AS change_rows,
  sum(length(payload)) AS logical_payload_bytes,
  sum(CAST(xxhash64(id, bucket, amount, revision, payload, _change_type) AS DECIMAL(38, 0))) AS hash_sum,
  sum(CAST(xxhash64(_change_type, payload, revision, amount, bucket, id) AS DECIMAL(38, 0))) AS reverse_hash_sum,
  min(id) AS minimum_id,
  max(id) AS maximum_id
FROM table_changes('main.automatic_cdf_20261010.demo_storage_automatic', 2)
GROUP BY _change_type
ORDER BY _change_type;

-- COMMAND ----------

SELECT
  'classic' AS cdf_mode,
  _change_type,
  count(*) AS change_rows,
  sum(length(payload)) AS logical_payload_bytes,
  sum(CAST(xxhash64(id, bucket, amount, revision, payload, _change_type) AS DECIMAL(38, 0))) AS hash_sum,
  sum(CAST(xxhash64(_change_type, payload, revision, amount, bucket, id) AS DECIMAL(38, 0))) AS reverse_hash_sum,
  min(id) AS minimum_id,
  max(id) AS maximum_id
FROM table_changes('main.automatic_cdf_20261010.demo_storage_classic', 2)
GROUP BY _change_type
ORDER BY _change_type;

-- COMMAND ----------

SELECT
  'classic' AS cdf_mode,
  _change_type,
  count(*) AS change_rows,
  sum(length(payload)) AS logical_payload_bytes,
  sum(CAST(xxhash64(id, bucket, amount, revision, payload, _change_type) AS DECIMAL(38, 0))) AS hash_sum,
  sum(CAST(xxhash64(_change_type, payload, revision, amount, bucket, id) AS DECIMAL(38, 0))) AS reverse_hash_sum,
  min(id) AS minimum_id,
  max(id) AS maximum_id
FROM table_changes('main.automatic_cdf_20261010.demo_storage_classic', 2)
GROUP BY _change_type
ORDER BY _change_type;

-- COMMAND ----------

SELECT
  'automatic' AS cdf_mode,
  _change_type,
  count(*) AS change_rows,
  sum(length(payload)) AS logical_payload_bytes,
  sum(CAST(xxhash64(id, bucket, amount, revision, payload, _change_type) AS DECIMAL(38, 0))) AS hash_sum,
  sum(CAST(xxhash64(_change_type, payload, revision, amount, bucket, id) AS DECIMAL(38, 0))) AS reverse_hash_sum,
  min(id) AS minimum_id,
  max(id) AS maximum_id
FROM table_changes('main.automatic_cdf_20261010.demo_storage_automatic', 2)
GROUP BY _change_type
ORDER BY _change_type;
