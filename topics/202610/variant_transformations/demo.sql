-- Databricks notebook source
WITH orders AS (
  SELECT parse_json('{"status":"new"}') AS order_data
)
SELECT
  order_data AS before,
  variant_set(order_data, '$.status', 'paid') AS after
FROM orders;

-- COMMAND ----------

SELECT variant_insert(
  parse_json('{"status":"new"}'),
  '$.priority', 'high'
) AS result;

-- COMMAND ----------

SELECT variant_delete(
  parse_json('{"status":"new","debug":true}'),
  '$.debug'
) AS result;

-- COMMAND ----------

SELECT variant_array_append(
  parse_json('{"items":["tea"]}'),
  '$.items', 'coffee'
) AS result;

-- COMMAND ----------

SELECT variant_strip_nulls(
  parse_json('{"note":null,"items":["tea",null]}')
) AS result;

-- COMMAND ----------

SELECT variant_strip_nulls(
  parse_json('{"note":null,"items":["tea",null]}'),
  false
) AS result;

-- COMMAND ----------

SELECT
  try_variant_set(
    parse_json('{"price":10}'), '$.price.currency', 'EUR'
  ) AS incompatible_set,
  try_variant_insert(
    parse_json('{"status":"new"}'), '$.status', 'paid'
  ) AS existing_insert_key,
  try_variant_array_append(
    parse_json('{"items":1}'), '$.items', 'tea'
  ) AS non_array_append;
