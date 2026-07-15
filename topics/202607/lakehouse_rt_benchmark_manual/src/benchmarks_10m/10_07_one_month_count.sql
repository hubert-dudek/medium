SELECT
  COUNT(*) AS transaction_count
FROM rt_sql_bench_10m.transactions AS t
WHERE t.transaction_date BETWEEN DATE '2024-06-01' AND DATE '2024-06-30';
