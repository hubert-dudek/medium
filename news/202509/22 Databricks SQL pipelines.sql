-- Databricks notebook source
-- MAGIC %md
-- MAGIC stream / materialized view like in dlt

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW orders_amount_date
AS
SELECT
  order_date,
  SUM(amount) AS total_order_amount
FROM orders
GROUP BY order_date;
