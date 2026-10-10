-- Databricks notebook source
SELECT _change_type, count(*) AS row_count
FROM table_changes('main.automatic_cdf_20261010.storage_full_20261010_01_legacy', 1, 1)
GROUP BY _change_type;

-- COMMAND ----------

SELECT _change_type, count(*) AS row_count
FROM table_changes('main.automatic_cdf_20261010.storage_full_20261010_01_auto', 1, 1)
GROUP BY _change_type;

-- COMMAND ----------

SELECT _change_type,
       count(*) AS row_count,
       sum(length(payload)) AS payload_bytes,
       sum(cast(xxhash64(id, bucket, amount, revision, payload, _change_type) AS DECIMAL(38,0))) AS hash_sum,
       sum(cast(xxhash64(_change_type, payload, revision, amount, bucket, id) AS DECIMAL(38,0))) AS reverse_hash_sum,
       min(id) AS minimum_id,
       max(id) AS maximum_id,
       min(revision) AS minimum_revision,
       max(revision) AS maximum_revision
FROM table_changes('main.automatic_cdf_20261010.storage_full_20261010_01_legacy', 1, 1)
GROUP BY _change_type;

-- COMMAND ----------

SELECT _change_type,
       count(*) AS row_count,
       sum(length(payload)) AS payload_bytes,
       sum(cast(xxhash64(id, bucket, amount, revision, payload, _change_type) AS DECIMAL(38,0))) AS hash_sum,
       sum(cast(xxhash64(_change_type, payload, revision, amount, bucket, id) AS DECIMAL(38,0))) AS reverse_hash_sum,
       min(id) AS minimum_id,
       max(id) AS maximum_id,
       min(revision) AS minimum_revision,
       max(revision) AS maximum_revision
FROM table_changes('main.automatic_cdf_20261010.storage_full_20261010_01_auto', 1, 1)
GROUP BY _change_type;

-- COMMAND ----------

SELECT _change_type, count(*) AS row_count
FROM table_changes('main.automatic_cdf_20261010.bench_large_20261010_legacy', 2, 16)
GROUP BY _change_type;

-- COMMAND ----------

SELECT _change_type, count(*) AS row_count
FROM table_changes('main.automatic_cdf_20261010.bench_large_20261010_auto', 2, 16)
GROUP BY _change_type;

-- COMMAND ----------

SELECT _change_type,
       count(*) AS row_count,
       sum(length(payload)) AS payload_bytes,
       sum(cast(xxhash64(id, bucket, amount, revision, payload, _change_type) AS DECIMAL(38,0))) AS hash_sum,
       sum(cast(xxhash64(_change_type, payload, revision, amount, bucket, id) AS DECIMAL(38,0))) AS reverse_hash_sum,
       min(id) AS minimum_id,
       max(id) AS maximum_id,
       min(revision) AS minimum_revision,
       max(revision) AS maximum_revision
FROM table_changes('main.automatic_cdf_20261010.bench_large_20261010_legacy', 2, 16)
GROUP BY _change_type;

-- COMMAND ----------

SELECT _change_type,
       count(*) AS row_count,
       sum(length(payload)) AS payload_bytes,
       sum(cast(xxhash64(id, bucket, amount, revision, payload, _change_type) AS DECIMAL(38,0))) AS hash_sum,
       sum(cast(xxhash64(_change_type, payload, revision, amount, bucket, id) AS DECIMAL(38,0))) AS reverse_hash_sum,
       min(id) AS minimum_id,
       max(id) AS maximum_id,
       min(revision) AS minimum_revision,
       max(revision) AS maximum_revision
FROM table_changes('main.automatic_cdf_20261010.bench_large_20261010_auto', 2, 16)
GROUP BY _change_type;

-- COMMAND ----------

SELECT _change_type, count(*) AS row_count
FROM table_changes('main.automatic_cdf_20261010.storage_full_20261010_01_legacy', 2, 2)
GROUP BY _change_type;

-- COMMAND ----------

SELECT _change_type, count(*) AS row_count
FROM table_changes('main.automatic_cdf_20261010.storage_full_20261010_01_auto', 2, 2)
GROUP BY _change_type;

-- COMMAND ----------

SELECT _change_type,
       count(*) AS row_count,
       sum(length(payload)) AS payload_bytes,
       sum(cast(xxhash64(id, bucket, amount, revision, payload, _change_type) AS DECIMAL(38,0))) AS hash_sum,
       sum(cast(xxhash64(_change_type, payload, revision, amount, bucket, id) AS DECIMAL(38,0))) AS reverse_hash_sum,
       min(id) AS minimum_id,
       max(id) AS maximum_id,
       min(revision) AS minimum_revision,
       max(revision) AS maximum_revision
FROM table_changes('main.automatic_cdf_20261010.storage_full_20261010_01_legacy', 2, 2)
GROUP BY _change_type;

-- COMMAND ----------

SELECT _change_type,
       count(*) AS row_count,
       sum(length(payload)) AS payload_bytes,
       sum(cast(xxhash64(id, bucket, amount, revision, payload, _change_type) AS DECIMAL(38,0))) AS hash_sum,
       sum(cast(xxhash64(_change_type, payload, revision, amount, bucket, id) AS DECIMAL(38,0))) AS reverse_hash_sum,
       min(id) AS minimum_id,
       max(id) AS maximum_id,
       min(revision) AS minimum_revision,
       max(revision) AS maximum_revision
FROM table_changes('main.automatic_cdf_20261010.storage_full_20261010_01_auto', 2, 2)
GROUP BY _change_type;

-- COMMAND ----------

SELECT _change_type, count(*) AS row_count
FROM table_changes('main.automatic_cdf_20261010.storage_full_20261010_01_legacy', 2, 4)
GROUP BY _change_type;

-- COMMAND ----------

SELECT _change_type, count(*) AS row_count
FROM table_changes('main.automatic_cdf_20261010.storage_full_20261010_01_auto', 2, 4)
GROUP BY _change_type;

-- COMMAND ----------

SELECT _change_type,
       count(*) AS row_count,
       sum(length(payload)) AS payload_bytes,
       sum(cast(xxhash64(id, bucket, amount, revision, payload, _change_type) AS DECIMAL(38,0))) AS hash_sum,
       sum(cast(xxhash64(_change_type, payload, revision, amount, bucket, id) AS DECIMAL(38,0))) AS reverse_hash_sum,
       min(id) AS minimum_id,
       max(id) AS maximum_id,
       min(revision) AS minimum_revision,
       max(revision) AS maximum_revision
FROM table_changes('main.automatic_cdf_20261010.storage_full_20261010_01_legacy', 2, 4)
GROUP BY _change_type;

-- COMMAND ----------

SELECT _change_type,
       count(*) AS row_count,
       sum(length(payload)) AS payload_bytes,
       sum(cast(xxhash64(id, bucket, amount, revision, payload, _change_type) AS DECIMAL(38,0))) AS hash_sum,
       sum(cast(xxhash64(_change_type, payload, revision, amount, bucket, id) AS DECIMAL(38,0))) AS reverse_hash_sum,
       min(id) AS minimum_id,
       max(id) AS maximum_id,
       min(revision) AS minimum_revision,
       max(revision) AS maximum_revision
FROM table_changes('main.automatic_cdf_20261010.storage_full_20261010_01_auto', 2, 4)
GROUP BY _change_type;
