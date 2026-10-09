-- Databricks notebook source
SELECT
  array('A', 'B', 'C', 'D') AS original,
  trim_array(array('A', 'B', 'C', 'D'), 2) AS trimmed;

-- COMMAND ----------

SELECT
  array('A', 'B', 'C', 'D') AS original,
  trim_array(array('A', 'B', 'C', 'D'), 0) AS unchanged,
  trim_array(array('A', 'B', 'C', 'D'), 4) AS empty;
