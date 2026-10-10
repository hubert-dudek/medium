-- Databricks notebook source
CREATE TABLE main.automatic_cdf_20261010.row_tracking_sql_demo (
  id BIGINT,
  product STRING,
  quantity INT
)
USING DELTA
TBLPROPERTIES ('delta.enableRowTracking' = 'true');

-- COMMAND ----------

INSERT INTO main.automatic_cdf_20261010.row_tracking_sql_demo
VALUES
  (1, 'keyboard', 10),
  (2, 'mouse', 20),
  (3, 'monitor', 30);

-- COMMAND ----------

SELECT id, product, quantity, _metadata.row_id AS row_id,
       _metadata.row_commit_version AS row_commit_version
FROM main.automatic_cdf_20261010.row_tracking_sql_demo
ORDER BY id;

-- COMMAND ----------

UPDATE main.automatic_cdf_20261010.row_tracking_sql_demo
SET quantity = 21
WHERE id = 2;

-- COMMAND ----------

SELECT id, product, quantity, _metadata.row_id AS row_id,
       _metadata.row_commit_version AS row_commit_version
FROM main.automatic_cdf_20261010.row_tracking_sql_demo
ORDER BY id;

-- COMMAND ----------

DESCRIBE HISTORY main.automatic_cdf_20261010.row_tracking_sql_demo;
