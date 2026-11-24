-- Databricks notebook source
SELECT
  0.1D + 0.2D AS double_sum,
  CAST('0.1' AS DECFLOAT(16)) +
    CAST('0.2' AS DECFLOAT(16)) AS decfloat_sum;

-- COMMAND ----------

SELECT
  CAST('1E+500' AS DECFLOAT(16)) AS decfloat_16,
  CAST('1E+500' AS DECFLOAT(34)) AS decfloat_34;
