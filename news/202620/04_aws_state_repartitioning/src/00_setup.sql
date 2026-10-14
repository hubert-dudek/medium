-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS main.news_202620_state;

CREATE VOLUME IF NOT EXISTS main.news_202620_state.checkpoints;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS main.news_202620_state.pipeline_events (
  event_id BIGINT,
  batch_id INT
)
USING DELTA;

-- COMMAND ----------

SELECT *
FROM main.news_202620_state.pipeline_events
ORDER BY batch_id, event_id;
