-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.news_202621');

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.news_202621.wiki_orders')
USING DELTA
COMMENT 'One row per demo order. Amounts include tax. Cancelled orders are retained.'
AS SELECT * FROM VALUES
  (1, 'paid',      24.00),
  (2, 'cancelled', 12.50)
AS orders(order_id, status, gross_amount);

-- COMMAND ----------

SELECT * FROM IDENTIFIER(:catalog || '.news_202621.wiki_orders') ORDER BY order_id;
