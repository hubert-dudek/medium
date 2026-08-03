-- Synthetic 50 billion row transaction dataset.
-- All data generation is SQL-only and deterministic.

USE CATALOG IDENTIFIER(:catalog);

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:schema)
COMMENT 'Synthetic transaction benchmark dataset managed by a Databricks bundle';

USE SCHEMA IDENTIFIER(:schema);

CREATE TABLE customers
CLUSTER BY AUTO
COMMENT 'Synthetic customer dimension used by join benchmarks'
TBLPROPERTIES (
  'benchmark.generator' = 'range',
  'benchmark.customer_count' = '500'
)
AS
SELECT
  CAST(id + 1 AS INT) AS customer_id,
  CONCAT('Customer ', LPAD(CAST(id + 1 AS STRING), 3, '0')) AS customer_name
FROM range(0, 500, 1, 1);

CREATE TABLE transactions
CLUSTER BY (transaction_date)
COMMENT 'Synthetic fact table clustered by transaction date and customer ID'
TBLPROPERTIES (
  'benchmark.generator' = 'range',
  'benchmark.row_count' = '50000000000',
  'benchmark.customer_count' = '500',
  'benchmark.start_date' = '2020-01-01',
  'benchmark.date_span_days' = '1827',
  'benchmark.range_partitions' = '40960'
)
AS
SELECT
  CAST(id + 1 AS BIGINT) AS transaction_id,
  CAST(PMOD(id, 500) + 1 AS INT) AS customer_id,
  DATE_ADD(
    DATE '2020-01-01',
    CAST(PMOD(id, 1827) AS INT)
  ) AS transaction_date,
  CAST(
    (
      PMOD(
        id * 37 + PMOD(id, 500) * 101,
        100000
      ) + 100
    ) / 100.0
    AS DECIMAL(18, 2)
  ) AS amount
FROM range(0, 50000000000, 1, 40960);
