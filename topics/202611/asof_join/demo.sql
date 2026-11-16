-- Databricks notebook source
SET spark.sql.join.asofJoin.enabled=true;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW transactions(transaction_id, customer_id, transaction_time) AS
VALUES
  (101, 1, TIMESTAMP '2026-11-16 08:45:00'),
  (102, 1, TIMESTAMP '2026-11-16 09:45:00'),
  (103, 1, TIMESTAMP '2026-11-16 10:15:00');

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW risk_history(customer_id, updated_at, risk_score) AS
VALUES
  (1, TIMESTAMP '2026-11-16 09:00:00', 20),
  (1, TIMESTAMP '2026-11-16 10:00:00', 70);

-- COMMAND ----------

SELECT t.transaction_id, r.risk_score
FROM transactions t
LEFT ASOF JOIN risk_history r
  MATCH_CONDITION (t.transaction_time >= r.updated_at)
  ON t.customer_id = r.customer_id
ORDER BY t.transaction_id;
