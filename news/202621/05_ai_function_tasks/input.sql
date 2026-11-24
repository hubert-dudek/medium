-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS news_202621_ai_functions;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS news_202621_ai_functions.tickets
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
AS SELECT * FROM VALUES
  (1, 'My invoice has an unexpected charge.'),
  (2, 'The application crashes when I sign in.')
AS tickets(id, body);
