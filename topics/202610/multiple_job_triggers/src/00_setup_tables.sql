-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS main.multiple_triggers_demo;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS main.multiple_triggers_demo.orders
USING DELTA
AS SELECT
  1 AS order_id,
  1 AS customer_id,
  CAST(42.00 AS DECIMAL(10, 2)) AS amount;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS main.multiple_triggers_demo.customers
USING DELTA
AS SELECT
  1 AS customer_id,
  'Demo customer' AS customer_name;

-- COMMAND ----------

SELECT 'orders' AS table_name, COUNT(*) AS rows
FROM main.multiple_triggers_demo.orders
UNION ALL
SELECT 'customers' AS table_name, COUNT(*) AS rows
FROM main.multiple_triggers_demo.customers;
