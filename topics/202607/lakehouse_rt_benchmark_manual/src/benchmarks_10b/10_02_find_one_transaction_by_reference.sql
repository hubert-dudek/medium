-- find one transaction by unclustered reference_code in 10 billion transactions
-- Lakehouse//RT is SELECT-only; CURRENT_TIMESTAMP makes the measured SELECT non-deterministic

SELECT
  t.product_id
FROM rt_sql_bench_10b.transactions AS t
WHERE t.reference_code = 'R073811789012345'
LIMIT 1;
