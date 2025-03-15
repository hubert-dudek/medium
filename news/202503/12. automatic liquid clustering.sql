-- Databricks notebook source
CREATE OR REPLACE TABLE hubert_event_logs_liquid (
    company_name STRING,
    event_date DATE,
    event_count INT,
    region       STRING
)
CLUSTER BY AUTO; -- predictive - need stats

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![Liquid-Clusters-OG.png](./images/Liquid-Clusters-OG.png "Liquid-Clusters-OG.png")
