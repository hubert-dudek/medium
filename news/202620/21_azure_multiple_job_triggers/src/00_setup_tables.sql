-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS main.news_202620_multiple_triggers;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS main.news_202620_multiple_triggers.orders
AS SELECT 1 AS order_id, 'created' AS status;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS main.news_202620_multiple_triggers.customers
AS SELECT 1 AS customer_id, 'demo' AS name;
