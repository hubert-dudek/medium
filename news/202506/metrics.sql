-- Databricks notebook source
CREATE OR REPLACE TABLE transactions (
  tx_id            INT,
  tx_ts            TIMESTAMP,
  customer_id      INT,
  transaction_type INT,     -- 1,4,5 = sale; 3,6,7 = refund
  amount           DECIMAL(10,2)
);

INSERT INTO transactions VALUES
  (1, '2025-05-01 09:12', 101, 1, 100.00),
  (2, '2025-05-01 09:45', 101, 3,  30.00),
  (3, '2025-05-02 11:05', 102, 4,  50.00),
  (4, '2025-05-03 14:30', 103, 6,  10.00),
  (5, '2025-05-03 15:10', 101, 5,  80.00),
  (6, '2025-05-04 10:00', 104, 7,  40.00),
  (7, '2025-05-04 10:01', 104, 1, 120.00),
  (8, '2025-05-05 13:20', 105, 4,  60.00);


-- COMMAND ----------

SELECT * FROM transactions;

-- COMMAND ----------

CREATE OR REPLACE VIEW transaction_metrics
  (tx_id,
  tx_date,
   tx_type,
   sales_amount,
   refund_amount,
   net_sales)
WITH METRICS
LANGUAGE YAML
COMMENT 'Canonical sales and refund metrics for the shop.'
AS $$
version: 0.1
source: hub.default.transactions
filter: transaction_type IN (1,3,4,5,6,7)

dimensions:

  - name: tx_id
    expr: tx_id

  - name: tx_date
    expr: date(tx_ts)

  - name: tx_type
    expr: CASE
             WHEN transaction_type IN (1,4,5) THEN 'Sale'
             WHEN transaction_type IN (3,6,7) THEN 'Refund'
           END

measures:
  - name: sales_amount
    expr: COALESCE(SUM(amount) FILTER (WHERE transaction_type IN (1,4,5)),0)

  - name: refund_amount
    expr: COALESCE(SUM(amount) FILTER (WHERE transaction_type IN (3,6,7)),0)

  # Measures can reference earlier measures, so this stays DRY
  - name: net_sales
    expr: sales_amount - refund_amount
$$;


-- COMMAND ----------

SELECT
  tx_id,
  MEASURE(net_sales)
FROM
  transaction_metrics
WHERE
  tx_type = 'Sale'
GROUP BY
  tx_id

-- COMMAND ----------

SELECT
  tx_id,
  MEASURE(refund_amount)
FROM
  transaction_metrics
WHERE
  tx_type = 'Refund'
GROUP BY
  tx_id

-- COMMAND ----------

SELECT
  MEASURE(sales_amount),
  MEASURE(refund_amount),
  MEASURE(net_sales)
FROM
  transaction_metrics
WHERE
  tx_type = 'Sale'

-- COMMAND ----------

SELECT
  MEASURE(sales_amount),
  MEASURE(refund_amount),
  MEASURE(net_sales)
FROM
  transaction_metrics

-- COMMAND ----------

SELECT 
      tx_date,
       MEASURE(sales_amount)  AS sales,
       MEASURE(refund_amount) AS refunds,
       MEASURE(net_sales)     AS net
FROM transaction_metrics 
WHERE tx_type = 'Sale'
GROUP BY tx_date;

-- COMMAND ----------

SELECT
  MEASURE(sales_amount)
FROM
  transaction_metrics

-- COMMAND ----------

SELECT
  SUM(amount)
FROM
  transactions
WHERE
  transaction_type IN (1, 4, 5)

-- COMMAND ----------

SELECT
  MEASURE(refund_amount)
FROM
  transaction_metrics

-- COMMAND ----------

SELECT
  SUM(amount)
FROM
  transactions
WHERE
  transaction_type IN (3, 6, 7);

-- COMMAND ----------

SELECT
  MEASURE(net_sales)
FROM
  transaction_metrics;

-- COMMAND ----------

SELECT
    SUM(
        CASE
            WHEN transaction_type IN (1,4,5) THEN  amount
            WHEN transaction_type IN (3,6,7) THEN -amount
        END
    ) AS netsale
FROM transactions;
