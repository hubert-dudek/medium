-- find one transaction by clustered transaction_id in 10 billion transactions
-- Lakehouse//RT is SELECT-only; CURRENT_TIMESTAMP makes the measured SELECT non-deterministic

SELECT
  t.merchant_id
FROM rt_sql_bench_10m.transactions AS t
WHERE t.transaction_id = 5000335
LIMIT 1;
