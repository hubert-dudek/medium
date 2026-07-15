-- find one transaction from 10 billion transactions and join customer (100 million), merchant (10 million), product (25 million), and category (100 rows)
-- Lakehouse//RT is SELECT-only; CURRENT_TIMESTAMP makes the measured SELECT non-deterministic

SELECT
  '10_06_join_one_transaction_to_dimensions' AS benchmark_id,
  CURRENT_TIMESTAMP() AS benchmark_run_at,
  t.transaction_id,
  t.transaction_ts,
  t.amount,
  t.status,
  c.customer_key
FROM rt_sql_bench_10b.transactions AS t
INNER JOIN rt_sql_bench_10b.dim_customer AS c
  ON t.customer_id = c.customer_id
WHERE t.transaction_id = 5000000001
LIMIT 1;
