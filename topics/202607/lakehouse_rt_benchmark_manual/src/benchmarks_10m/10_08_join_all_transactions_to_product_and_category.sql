-- scan all 10 billion transactions, join product (25 million) and category (100 rows), aggregate by brand, and return 100 groups
-- Lakehouse//RT is SELECT-only; CURRENT_TIMESTAMP makes the measured SELECT non-deterministic

SELECT
  cat.category_group,
  p.brand_id,
  t.status,
  t.channel,
  SUM(t.amount) AS total_amount
FROM rt_sql_bench_10m.transactions AS t
INNER JOIN rt_sql_bench_10m.dim_product AS p
  ON t.product_id = p.product_id
INNER JOIN rt_sql_bench_10m.dim_category AS cat
  ON p.category_id = cat.category_id
GROUP BY cat.category_group, p.brand_id, t.status, t.channel
LIMIT 100;
