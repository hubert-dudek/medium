-- Databricks notebook source
CREATE OR REPLACE TABLE country_cdc (
  country_id    INT,
  country_name  STRING,
  operation     STRING,
  sequence_num  BIGINT
);

INSERT INTO country_cdc VALUES
  (1, 'Czech Republic', 'UPSERT', 1),
  (1, 'Czechia',        'UPSERT', 2),
  (2, 'Slovakia',       'UPSERT', 1),
  (1, 'Czech Republic', 'UPSERT', 0)

-- COMMAND ----------

SELECT * FROM dim_country_cdc;