-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS main.news_202620_automatic_cdf;

-- COMMAND ----------

DROP TABLE IF EXISTS main.news_202620_automatic_cdf.classic_cdf;
DROP TABLE IF EXISTS main.news_202620_automatic_cdf.automatic_cdf;

CREATE TABLE main.news_202620_automatic_cdf.classic_cdf (
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

CREATE TABLE main.news_202620_automatic_cdf.automatic_cdf (
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

INSERT INTO main.news_202620_automatic_cdf.classic_cdf
VALUES (1, 'keyboard', 10), (2, 'mouse', 20), (3, 'monitor', 30);

INSERT INTO main.news_202620_automatic_cdf.automatic_cdf
VALUES (1, 'keyboard', 10), (2, 'mouse', 20), (3, 'monitor', 30);

-- COMMAND ----------

UPDATE main.news_202620_automatic_cdf.classic_cdf
SET quantity = 21 WHERE id = 2;

UPDATE main.news_202620_automatic_cdf.automatic_cdf
SET quantity = 21 WHERE id = 2;

-- COMMAND ----------

DELETE FROM main.news_202620_automatic_cdf.classic_cdf WHERE id = 3;
DELETE FROM main.news_202620_automatic_cdf.automatic_cdf WHERE id = 3;

-- COMMAND ----------

SELECT id, product, quantity, _change_type, _commit_version
FROM table_changes('main.news_202620_automatic_cdf.classic_cdf', 1)
ORDER BY _commit_version, id, _change_type;

-- COMMAND ----------

SELECT id, product, quantity, _change_type, _commit_version
FROM table_changes('main.news_202620_automatic_cdf.automatic_cdf', 1)
ORDER BY _commit_version, id, _change_type;
