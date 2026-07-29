USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

SET use_cached_result = false; 

EXPLAIN SELECT
  AVG(amount) AS average_amount
FROM transactions
WHERE transaction_date BETWEEN '2024-06-01' AND '2024-07-01';

SELECT
  AVG(amount) AS average_amount
FROM transactions
WHERE transaction_date BETWEEN '2024-06-01' AND '2024-07-01';