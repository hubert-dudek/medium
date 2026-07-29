USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

SET use_cached_result = false;

EXPLAIN SELECT
  amount
FROM transactions
WHERE transaction_id = 777777;

SELECT
  amount
FROM transactions
WHERE transaction_id = 777777;