SELECT
  t.merchant_id
FROM rt_sql_bench_10m.transactions AS t
WHERE t.transaction_id = 6000335
LIMIT 1;