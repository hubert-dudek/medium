-- Databricks notebook source
SELECT parse_sql('SELECT 42 AS answer') AS parsed_sql;

-- COMMAND ----------

SELECT parse_sql('
  SELECT upper(name) AS customer, amount * 1.2 AS total
  FROM orders
  WHERE amount > :minimum
') AS parsed_sql;

-- COMMAND ----------

SELECT parse_sql('SELECT 1; SELEC 2; SELECT 3') AS parsed_sql;
