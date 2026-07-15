-- return the latest 100 transactions for customer 42 from 10 billion transactions
-- Lakehouse//RT is SELECT-only; CURRENT_TIMESTAMP makes the measured SELECT non-deterministic

SELECT
  t.transaction_ts
FROM rt_sql_bench_10m.transactions AS t
WHERE t.customer_id = 42
ORDER BY t.transaction_id DESC
LIMIT 100;
