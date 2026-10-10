-- Databricks notebook source
CREATE TABLE main.automatic_cdf_20261010.demo_mixed_classic (
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

CREATE TABLE main.automatic_cdf_20261010.demo_mixed_automatic (
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

INSERT INTO main.automatic_cdf_20261010.demo_mixed_classic
SELECT id, bucket, amount, revision, payload
FROM main.automatic_cdf_20261010.demo_storage_source;

-- COMMAND ----------

INSERT INTO main.automatic_cdf_20261010.demo_mixed_automatic
SELECT id, bucket, amount, revision, payload
FROM main.automatic_cdf_20261010.demo_storage_source;

-- COMMAND ----------

DESCRIBE DETAIL main.automatic_cdf_20261010.demo_mixed_classic;

-- COMMAND ----------

DESCRIBE DETAIL main.automatic_cdf_20261010.demo_mixed_automatic;

-- COMMAND ----------

INSERT INTO main.automatic_cdf_20261010.demo_mixed_classic
SELECT id + 5000000, bucket, amount, revision, payload
FROM main.automatic_cdf_20261010.demo_storage_source
WHERE id < 250000;

-- COMMAND ----------

INSERT INTO main.automatic_cdf_20261010.demo_mixed_automatic
SELECT id + 5000000, bucket, amount, revision, payload
FROM main.automatic_cdf_20261010.demo_storage_source
WHERE id < 250000;

-- COMMAND ----------

UPDATE main.automatic_cdf_20261010.demo_mixed_automatic
SET amount = amount + 1, revision = 1
WHERE id < 5000000 AND pmod(id, 10) = 0;

-- COMMAND ----------

UPDATE main.automatic_cdf_20261010.demo_mixed_classic
SET amount = amount + 1, revision = 1
WHERE id < 5000000 AND pmod(id, 10) = 0;

-- COMMAND ----------

DELETE FROM main.automatic_cdf_20261010.demo_mixed_classic
WHERE id < 5000000 AND pmod(id, 100) = 51;

-- COMMAND ----------

DELETE FROM main.automatic_cdf_20261010.demo_mixed_automatic
WHERE id < 5000000 AND pmod(id, 100) = 51;

-- COMMAND ----------

INSERT INTO main.automatic_cdf_20261010.demo_mixed_automatic
SELECT id + 5250000, bucket, amount, revision, payload
FROM main.automatic_cdf_20261010.demo_storage_source
WHERE id < 250000;

-- COMMAND ----------

INSERT INTO main.automatic_cdf_20261010.demo_mixed_classic
SELECT id + 5250000, bucket, amount, revision, payload
FROM main.automatic_cdf_20261010.demo_storage_source
WHERE id < 250000;

-- COMMAND ----------

UPDATE main.automatic_cdf_20261010.demo_mixed_classic
SET amount = amount + 2, revision = 2
WHERE id < 5000000 AND pmod(id, 10) = 0;

-- COMMAND ----------

UPDATE main.automatic_cdf_20261010.demo_mixed_automatic
SET amount = amount + 2, revision = 2
WHERE id < 5000000 AND pmod(id, 10) = 0;

-- COMMAND ----------

DELETE FROM main.automatic_cdf_20261010.demo_mixed_automatic
WHERE id < 5000000 AND pmod(id, 100) = 52;

-- COMMAND ----------

DELETE FROM main.automatic_cdf_20261010.demo_mixed_classic
WHERE id < 5000000 AND pmod(id, 100) = 52;

-- COMMAND ----------

INSERT INTO main.automatic_cdf_20261010.demo_mixed_classic
SELECT id + 5500000, bucket, amount, revision, payload
FROM main.automatic_cdf_20261010.demo_storage_source
WHERE id < 250000;

-- COMMAND ----------

INSERT INTO main.automatic_cdf_20261010.demo_mixed_automatic
SELECT id + 5500000, bucket, amount, revision, payload
FROM main.automatic_cdf_20261010.demo_storage_source
WHERE id < 250000;

-- COMMAND ----------

UPDATE main.automatic_cdf_20261010.demo_mixed_automatic
SET amount = amount + 3, revision = 3
WHERE id < 5000000 AND pmod(id, 10) = 0;

-- COMMAND ----------

UPDATE main.automatic_cdf_20261010.demo_mixed_classic
SET amount = amount + 3, revision = 3
WHERE id < 5000000 AND pmod(id, 10) = 0;

-- COMMAND ----------

DELETE FROM main.automatic_cdf_20261010.demo_mixed_classic
WHERE id < 5000000 AND pmod(id, 100) = 53;

-- COMMAND ----------

DELETE FROM main.automatic_cdf_20261010.demo_mixed_automatic
WHERE id < 5000000 AND pmod(id, 100) = 53;

-- COMMAND ----------

DESCRIBE DETAIL main.automatic_cdf_20261010.demo_mixed_classic;

-- COMMAND ----------

DESCRIBE DETAIL main.automatic_cdf_20261010.demo_mixed_automatic;

-- COMMAND ----------

SELECT _change_type, count(*) AS change_rows
FROM table_changes('main.automatic_cdf_20261010.demo_mixed_classic', 2)
GROUP BY _change_type
ORDER BY _change_type;

-- COMMAND ----------

SELECT _change_type, count(*) AS change_rows
FROM table_changes('main.automatic_cdf_20261010.demo_mixed_automatic', 2)
GROUP BY _change_type
ORDER BY _change_type;

-- COMMAND ----------

DESCRIBE HISTORY main.automatic_cdf_20261010.demo_mixed_classic;

-- COMMAND ----------

DESCRIBE HISTORY main.automatic_cdf_20261010.demo_mixed_automatic;
