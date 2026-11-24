-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS sbx_test_data.news_202621_quality;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sbx_test_data.news_202621_quality.sales
USING DELTA AS
SELECT * FROM VALUES
  (1, 10.0),
  (2, 20.0),
  (3, 30.0),
  (4, NULL)
AS sales(order_id, amount);
