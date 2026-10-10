-- Databricks notebook source
CREATE TABLE main.automatic_cdf_20261010.demo_cdf_classic (
  id BIGINT,
  product STRING,
  quantity INT
)
USING DELTA
TBLPROPERTIES (
  'delta.enableRowTracking' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableChangeDataFeed' = 'true'
);

-- COMMAND ----------

CREATE TABLE main.automatic_cdf_20261010.demo_cdf_automatic (
  id BIGINT,
  product STRING,
  quantity INT
)
USING DELTA
TBLPROPERTIES (
  'delta.enableRowTracking' = 'true',
  'delta.enableDeletionVectors' = 'true'
);

-- COMMAND ----------

INSERT INTO main.automatic_cdf_20261010.demo_cdf_classic
VALUES (1, 'keyboard', 10), (2, 'mouse', 20), (3, 'monitor', 30);

-- COMMAND ----------

INSERT INTO main.automatic_cdf_20261010.demo_cdf_automatic
VALUES (1, 'keyboard', 10), (2, 'mouse', 20), (3, 'monitor', 30);

-- COMMAND ----------

UPDATE main.automatic_cdf_20261010.demo_cdf_classic
SET quantity = 21
WHERE id = 2;

-- COMMAND ----------

UPDATE main.automatic_cdf_20261010.demo_cdf_automatic
SET quantity = 21
WHERE id = 2;

-- COMMAND ----------

DELETE FROM main.automatic_cdf_20261010.demo_cdf_classic
WHERE id = 3;

-- COMMAND ----------

DELETE FROM main.automatic_cdf_20261010.demo_cdf_automatic
WHERE id = 3;

-- COMMAND ----------

SELECT *
FROM table_changes('main.automatic_cdf_20261010.demo_cdf_classic', 1)
ORDER BY _commit_version, id, _change_type;

-- COMMAND ----------

SELECT *
FROM table_changes('main.automatic_cdf_20261010.demo_cdf_automatic', 1)
ORDER BY _commit_version, id, _change_type;
