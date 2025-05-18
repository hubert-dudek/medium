-- Databricks notebook source
-- MAGIC %md
-- MAGIC string_agg, listagg

-- COMMAND ----------

SELECT
  string_agg(col)
FROM
  VALUES ('a'), ('b'), ('c') AS tab (col);
