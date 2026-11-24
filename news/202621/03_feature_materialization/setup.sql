-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS news_202621_materialization;

-- COMMAND ----------

CREATE OR REPLACE TABLE news_202621_materialization.transactions
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
AS SELECT
  customer_id,
  amount,
  CAST(date_sub(current_date(), days_ago) AS TIMESTAMP) AS event_ts
FROM VALUES
  (1, 100, 1),
  (1, 50, 2),
  (2, 75, 1)
AS source(customer_id, amount, days_ago);
