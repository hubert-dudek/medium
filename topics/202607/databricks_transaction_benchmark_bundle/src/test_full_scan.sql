USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

SET use_cached_result = false;

EXPLAIN SELECT
  SUM(amount) AS total_amount
FROM transactions;

SELECT
  SUM(amount) AS total_amount
FROM transactions;
