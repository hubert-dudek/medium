-- scan all 10 billion transactions and group by status, channel, and currency
-- Lakehouse//RT is SELECT-only; CURRENT_TIMESTAMP makes the measured SELECT non-deterministic

SELECT
  t.status,
  t.channel,
  t.currency,
  COUNT(*) AS transaction_count
FROM rt_sql_bench_10m.transactions AS t
GROUP BY t.status, t.channel, t.currency
ORDER BY transaction_count DESC, t.status, t.channel, t.currency
LIMIT 100;
