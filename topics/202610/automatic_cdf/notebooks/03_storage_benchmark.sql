-- Databricks notebook source
CREATE TABLE main.automatic_cdf_20261010.demo_storage_source
USING DELTA
AS
SELECT /*+ REPARTITION(16) */
  id,
  CAST(pmod(id, 100) AS INT) AS bucket,
  id * 17 AS amount,
  0 AS revision,
  concat(
    unhex(sha2(concat_ws(':', CAST(id AS STRING), '0'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), '1'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), '2'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), '3'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), '4'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), '5'), 256))
  ) AS payload
FROM range(5000000);

-- COMMAND ----------

CREATE TABLE main.automatic_cdf_20261010.demo_storage_classic (
  id BIGINT,
  bucket INT,
  amount BIGINT,
  revision INT,
  payload BINARY
)
USING DELTA
TBLPROPERTIES (
  'delta.enableRowTracking' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'false',
  'delta.autoOptimize.autoCompact' = 'false',
  'delta.enableChangeDataFeed' = 'true'
);

-- COMMAND ----------

CREATE TABLE main.automatic_cdf_20261010.demo_storage_automatic (
  id BIGINT,
  bucket INT,
  amount BIGINT,
  revision INT,
  payload BINARY
)
USING DELTA
TBLPROPERTIES (
  'delta.enableRowTracking' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'false',
  'delta.autoOptimize.autoCompact' = 'false'
);

-- COMMAND ----------

INSERT INTO main.automatic_cdf_20261010.demo_storage_classic
SELECT id, bucket, amount, revision, payload
FROM main.automatic_cdf_20261010.demo_storage_source;

-- COMMAND ----------

INSERT INTO main.automatic_cdf_20261010.demo_storage_automatic
SELECT id, bucket, amount, revision, payload
FROM main.automatic_cdf_20261010.demo_storage_source;

-- COMMAND ----------

DESCRIBE DETAIL main.automatic_cdf_20261010.demo_storage_classic;

-- COMMAND ----------

DESCRIBE DETAIL main.automatic_cdf_20261010.demo_storage_automatic;

-- COMMAND ----------

UPDATE main.automatic_cdf_20261010.demo_storage_classic
SET
  amount = id * 17 + 1,
  revision = 1,
  payload = concat(
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '1', '0'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '1', '1'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '1', '2'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '1', '3'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '1', '4'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '1', '5'), 256))
  );

-- COMMAND ----------

UPDATE main.automatic_cdf_20261010.demo_storage_automatic
SET
  amount = id * 17 + 1,
  revision = 1,
  payload = concat(
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '1', '0'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '1', '1'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '1', '2'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '1', '3'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '1', '4'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '1', '5'), 256))
  );

-- COMMAND ----------

UPDATE main.automatic_cdf_20261010.demo_storage_automatic
SET
  amount = id * 17 + 2,
  revision = 2,
  payload = concat(
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '2', '0'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '2', '1'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '2', '2'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '2', '3'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '2', '4'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '2', '5'), 256))
  );

-- COMMAND ----------

UPDATE main.automatic_cdf_20261010.demo_storage_classic
SET
  amount = id * 17 + 2,
  revision = 2,
  payload = concat(
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '2', '0'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '2', '1'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '2', '2'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '2', '3'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '2', '4'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '2', '5'), 256))
  );

-- COMMAND ----------

UPDATE main.automatic_cdf_20261010.demo_storage_classic
SET
  amount = id * 17 + 3,
  revision = 3,
  payload = concat(
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '3', '0'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '3', '1'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '3', '2'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '3', '3'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '3', '4'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '3', '5'), 256))
  );

-- COMMAND ----------

UPDATE main.automatic_cdf_20261010.demo_storage_automatic
SET
  amount = id * 17 + 3,
  revision = 3,
  payload = concat(
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '3', '0'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '3', '1'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '3', '2'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '3', '3'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '3', '4'), 256)),
    unhex(sha2(concat_ws(':', CAST(id AS STRING), 'storage-update', '3', '5'), 256))
  );

-- COMMAND ----------

DESCRIBE DETAIL main.automatic_cdf_20261010.demo_storage_classic;

-- COMMAND ----------

DESCRIBE DETAIL main.automatic_cdf_20261010.demo_storage_automatic;

-- COMMAND ----------

SELECT
  _change_type,
  count(*) AS change_rows
FROM table_changes('main.automatic_cdf_20261010.demo_storage_classic', 2)
GROUP BY _change_type
ORDER BY _change_type;

-- COMMAND ----------

SELECT
  _change_type,
  count(*) AS change_rows
FROM table_changes('main.automatic_cdf_20261010.demo_storage_automatic', 2)
GROUP BY _change_type
ORDER BY _change_type;

-- COMMAND ----------

DESCRIBE HISTORY main.automatic_cdf_20261010.demo_storage_classic;

-- COMMAND ----------

DESCRIBE HISTORY main.automatic_cdf_20261010.demo_storage_automatic;
