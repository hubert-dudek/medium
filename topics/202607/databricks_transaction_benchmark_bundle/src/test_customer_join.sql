USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

SET use_cached_result = false; 

EXPLAIN SELECT
  c.customer_id,
  c.customer_name,
  SUM(t.amount) AS total_amount
FROM transactions AS t
INNER JOIN customers AS c
  ON t.customer_id = c.customer_id
GROUP BY
  c.customer_id,
  c.customer_name
ORDER BY
  c.customer_id;

SELECT
  c.customer_id,
  c.customer_name,
  SUM(t.amount) AS total_amount
FROM transactions AS t
INNER JOIN customers AS c
  ON t.customer_id = c.customer_id
GROUP BY
  c.customer_id,
  c.customer_name
ORDER BY
  c.customer_id;
