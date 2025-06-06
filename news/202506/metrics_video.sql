-- Databricks notebook source
-- DBTITLE 1,transaction table
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

-- DBTITLE 1,transaction data
SELECT * FROM transactions;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1: Start with one measure - get total sales = transaction_type IN (1,4,5)

-- COMMAND ----------

-- DBTITLE 1,0.1: Start with one measure
CREATE OR REPLACE VIEW transaction_metrics
  (sales_amount)
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1
source: hub.default.transactions
filter: transaction_type IN (1,3,4,5,6,7)


measures:
  - name: sales_amount
    expr: COALESCE(SUM(amount) FILTER (WHERE transaction_type IN (1,4,5)),0)
$$;


-- COMMAND ----------

-- DBTITLE 1,Total Sales Amount
SELECT
  MEASURE(sales_amount)
FROM
  transaction_metrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2: Add measure for refunds - get refunds total for transaction_type IN (3, 6, 7)

-- COMMAND ----------

-- DBTITLE 1,0.2: Add measure for refunds
CREATE OR REPLACE VIEW transaction_metrics
  (sales_amount,
  refund_amount)
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1
source: hub.default.transactions
filter: transaction_type IN (1,3,4,5,6,7)


measures:
  - name: sales_amount
    expr: COALESCE(SUM(amount) FILTER (WHERE transaction_type IN (1,4,5)),0)

  - name: refund_amount
    expr: COALESCE(SUM(amount) FILTER (WHERE transaction_type IN (3,6,7)),0)
$$;


-- COMMAND ----------

-- DBTITLE 1,Total refunds
SELECT
  MEASURE(refund_amount)
FROM
  transaction_metrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3: Calculate real sale: sales_amount minus refunds (DRY)

-- COMMAND ----------

-- DBTITLE 1,0.3: Calculate real sale: sales_amount minus refunds
CREATE OR REPLACE VIEW transaction_metrics
  (sales_amount,
  refund_amount,
  net_sales)
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1
source: hub.default.transactions
filter: transaction_type IN (1,3,4,5,6,7)


measures:
  - name: sales_amount
    expr: COALESCE(SUM(amount) FILTER (WHERE transaction_type IN (1,4,5)),0)

  - name: refund_amount
    expr: COALESCE(SUM(amount) FILTER (WHERE transaction_type IN (3,6,7)),0)

  - name: net_sales
    expr: sales_amount - refund_amount
$$;


-- COMMAND ----------

-- DBTITLE 1,Total net sale
SELECT
  MEASURE(net_sales)
FROM
  transaction_metrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 4: let's add dimension per day (timestamp to date)

-- COMMAND ----------

-- DBTITLE 1,0.4: let's add dimension per day
CREATE OR REPLACE VIEW transaction_metrics
  (tx_date,
  sales_amount,
  refund_amount,
  net_sales)
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1
source: hub.default.transactions
filter: transaction_type IN (1,3,4,5,6,7)

dimensions:
  - name: tx_date
    expr: date(tx_ts)

measures:
  - name: sales_amount
    expr: COALESCE(SUM(amount) FILTER (WHERE transaction_type IN (1,4,5)),0)

  - name: refund_amount
    expr: COALESCE(SUM(amount) FILTER (WHERE transaction_type IN (3,6,7)),0)

  - name: net_sales
    expr: sales_amount - refund_amount
$$;


-- COMMAND ----------

-- DBTITLE 1,Net sale per day
SELECT
  tx_date,
  MEASURE(net_sales)
FROM
  transaction_metrics
GROUP BY
  tx_date

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 5: let's display granular data - each id (but we need to group anyway as it is dimension)

-- COMMAND ----------

-- DBTITLE 1,0.5 let's display granular data - each id
CREATE OR REPLACE VIEW transaction_metrics
  (tx_id,
  tx_date,
  sales_amount,
  refund_amount,
  net_sales)
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1
source: hub.default.transactions
filter: transaction_type IN (1,3,4,5,6,7)

dimensions:
  - name: tx_id    
    expr: tx_id

  - name: tx_date
    expr: date(tx_ts)

measures:
  - name: sales_amount
    expr: COALESCE(SUM(amount) FILTER (WHERE transaction_type IN (1,4,5)),0)

  - name: refund_amount
    expr: COALESCE(SUM(amount) FILTER (WHERE transaction_type IN (3,6,7)),0)

  - name: net_sales
    expr: sales_amount - refund_amount
$$;


-- COMMAND ----------

-- DBTITLE 1,each id
SELECT
  tx_id,
  MEASURE(sales_amount),
  MEASURE(refund_amount),
  MEASURE(net_sales)
FROM
  transaction_metrics
GROUP BY
  tx_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #  6: let's display only refunds - glanular per id - we need to add filter

-- COMMAND ----------

-- DBTITLE 1,0.6 let's display only refunds
CREATE OR REPLACE VIEW transaction_metrics
  (tx_id,
  tx_date,
  tx_type,
  sales_amount,
  refund_amount,
  net_sales)
WITH METRICS
LANGUAGE YAML
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

  - name: net_sales
    expr: sales_amount - refund_amount
$$;


-- COMMAND ----------

-- DBTITLE 1,refund transactions
SELECT
  tx_id,
  MEASURE(refund_amount)
FROM
  transaction_metrics
WHERE
  tx_type = 'Refund'
GROUP BY
  tx_id
