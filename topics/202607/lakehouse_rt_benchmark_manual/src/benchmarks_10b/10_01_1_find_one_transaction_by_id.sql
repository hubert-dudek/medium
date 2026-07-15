SELECT
  t.merchant_id
FROM rt_sql_bench_10b.transactions AS t
WHERE t.transaction_id = 7000000335
LIMIT 1;