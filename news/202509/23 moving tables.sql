-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1: c60d8c13-55c8-464d-9d60-fb07f9052fa3
-- MAGIC 2: b461fe5a-8cca-43ef-a0fa-df5576127f00

-- COMMAND ----------

ALTER MATERIALIZED VIEW sample_trips_pipeline1
  SET TBLPROPERTIES("pipelines.pipelineId"="b461fe5a-8cca-43ef-a0fa-df5576127f00");
