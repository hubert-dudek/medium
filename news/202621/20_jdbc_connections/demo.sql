-- Databricks notebook source
SHOW CONNECTIONS;

-- COMMAND ----------

SELECT *
FROM remote_query(:connection_name, query => 'SELECT 1 AS connected');
