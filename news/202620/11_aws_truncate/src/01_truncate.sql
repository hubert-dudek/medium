-- Databricks notebook source
SELECT
  123.987 AS original,
  truncate(123.987, 2) AS truncated,
  round(123.987, 2) AS rounded;

-- COMMAND ----------

SELECT
  -123.987 AS original,
  truncate(-123.987, 2) AS truncated;

-- COMMAND ----------

SELECT
  123.987 AS original,
  truncate(123.987, -1) AS tens,
  truncate(123.987) AS integer_part;
