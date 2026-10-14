-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS main.state_repartitioning_demo;

CREATE VOLUME IF NOT EXISTS main.state_repartitioning_demo.checkpoints;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS main.state_repartitioning_demo.pipeline_events (
  event_id BIGINT,
  batch_id INT
)
USING DELTA;

-- COMMAND ----------

SELECT *
FROM main.state_repartitioning_demo.pipeline_events
ORDER BY batch_id, event_id;
