-- Databricks notebook source
USE sbx_test_data.news_202621_materialization;

-- COMMAND ----------

SELECT customer_id, amount, event_ts
FROM transactions
ORDER BY customer_id, event_ts;

-- COMMAND ----------

SELECT customer_id, COUNT(amount) AS transaction_count_7d
FROM transactions
WHERE event_ts >= current_date() - INTERVAL 7 DAYS
  AND event_ts < current_date()
GROUP BY customer_id
ORDER BY customer_id;

-- COMMAND ----------

SHOW TABLES LIKE 'customer_features_*';
