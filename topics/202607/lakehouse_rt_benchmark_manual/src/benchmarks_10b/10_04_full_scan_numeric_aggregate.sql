-- scan all 10 billion transactions and return count, sum, average, minimum, and maximum
-- Lakehouse//RT is SELECT-only; CURRENT_TIMESTAMP makes the measured SELECT non-deterministic

SELECT
  SUM(t.fee_amount) AS total_fees
FROM rt_sql_bench_10b.transactions AS t;
