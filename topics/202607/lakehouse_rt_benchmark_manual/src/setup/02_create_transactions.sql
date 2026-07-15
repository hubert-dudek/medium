-- Databricks notebook source
-- Creates the deterministic transaction fact table as a Unity Catalog managed Delta table.
-- transaction_id is clustered for lookup tests; customer_id and transaction_date support selective analytics.

CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema || '.transactions')
USING DELTA
CLUSTER BY (transaction_id, customer_id, transaction_date)
AS
WITH generated AS (
  SELECT
    id,
    CAST(id + 1 AS BIGINT) AS transaction_id,
    CASE
      -- Reserve 100 deterministic rows for customer-slice benchmarks at every scale.
      WHEN id BETWEEN CAST(5000000 AS BIGINT) AND CAST(5000099 AS BIGINT) THEN CAST(42 AS BIGINT)
      ELSE CAST(PMOD(XXHASH64(id, 11), CAST(:customer_rows AS BIGINT)) + 1 AS BIGINT)
    END AS customer_id,
    CAST(PMOD(XXHASH64(id, 13), CAST(:merchant_rows AS BIGINT)) + 1 AS BIGINT) AS merchant_id,
    CAST(PMOD(XXHASH64(id, 17), CAST(:product_rows AS BIGINT)) + 1 AS BIGINT) AS product_id,
    DATE_ADD(DATE '2022-01-01', CAST(PMOD(XXHASH64(id, 19), 1461) AS INT)) AS transaction_date,
    CAST(PMOD(XXHASH64(id, 23), 86400) AS INT) AS seconds_in_day
  FROM RANGE(0, CAST(:transaction_rows AS BIGINT), 1, CAST(:fact_num_parts AS INT))
)
SELECT
  transaction_id,
  customer_id,
  merchant_id,
  product_id,
  transaction_date,
  TIMESTAMPADD(SECOND, seconds_in_day, CAST(transaction_date AS TIMESTAMP)) AS transaction_ts,
  CAST((PMOD(XXHASH64(id, 29), 500000) + 100) / 100.0 AS DECIMAL(18, 2)) AS amount,
  CAST(PMOD(XXHASH64(id, 31), 8) + 1 AS INT) AS quantity,
  CASE
    WHEN PMOD(XXHASH64(id, 37), 100) < 80 THEN 'COMPLETED'
    WHEN PMOD(XXHASH64(id, 37), 100) < 88 THEN 'PENDING'
    WHEN PMOD(XXHASH64(id, 37), 100) < 97 THEN 'DECLINED'
    ELSE 'REFUNDED'
  END AS status,
  CASE CAST(PMOD(XXHASH64(id, 41), 4) AS INT)
    WHEN 0 THEN 'WEB'
    WHEN 1 THEN 'MOBILE'
    WHEN 2 THEN 'POS'
    ELSE 'API'
  END AS channel,
  CASE CAST(PMOD(XXHASH64(id, 43), 4) AS INT)
    WHEN 0 THEN 'EUR'
    WHEN 1 THEN 'USD'
    WHEN 2 THEN 'GBP'
    ELSE 'JPY'
  END AS currency,
  CASE WHEN PMOD(XXHASH64(id, 47), 1000) = 0 THEN TRUE ELSE FALSE END AS is_fraud,
  -- A modular permutation keeps the key unique while decorrelating it from clustered transaction_id.
  CONCAT(
    'R',
    LPAD(
      CAST(
        PMOD(
          CAST(id AS DECIMAL(20, 0)) * CAST(32416190071 AS DECIMAL(11, 0))
            + CAST(123456789012345 AS DECIMAL(15, 0)),
          CAST(1000000000000000 AS DECIMAL(16, 0))
        ) AS STRING
      ),
      15,
      '0'
    )
  ) AS reference_code,
  CAST(PMOD(XXHASH64(id, 53), 1001) AS INT) AS device_score,
  CAST((PMOD(XXHASH64(id, 59), 5000) + 1) / 100.0 AS DECIMAL(18, 2)) AS fee_amount
FROM generated;

