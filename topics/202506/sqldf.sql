-- Databricks notebook source
SELECT * FROM transactions;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC _sqldf.plot(kind="bar", x="cusomer_id", y="amount")

-- COMMAND ----------

MERGE INTO transactions AS target
USING (
  SELECT * FROM VALUES
  (1, '2025-06-28 00:00:00', 123, 1, 100.0),
  (2, '2025-06-28 00:00:00', 456, 2, 200.0)
 AS source (tx_id, tx_ts, customer_id, transaction_type, amount))
ON target.tx_id = source.tx_id
WHEN MATCHED THEN
  UPDATE SET target.tx_ts = source.tx_ts, target.customer_id = source.customer_id, target.transaction_type = source.transaction_type, target.amount = source.amount
WHEN NOT MATCHED THEN
  INSERT (tx_id, tx_ts, customer_id, transaction_type, amount) VALUES (source.tx_id, source.tx_ts, source.customer_id, source.transaction_type, source.amount);

-- COMMAND ----------

SELECT * FROM _sqldf
