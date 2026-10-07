-- Databricks notebook source
SELECT parse_sql('
  SELECT o.id, c.name
  FROM orders o
  JOIN customers c ON o.customer_id = c.id
') AS parsed_sql;

-- COMMAND ----------

SELECT parse_sql('
  WITH totals AS (
    SELECT customer_id, sum(amount) AS total
    FROM orders
    GROUP BY customer_id
  )
  SELECT c.name, t.total
  FROM totals t
  JOIN customers c ON t.customer_id = c.id
') AS parsed_sql;

-- COMMAND ----------

SELECT get_json_object(
  parse_sql('SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id'),
  '$[0].source_table_references'
) AS source_tables;