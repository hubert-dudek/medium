-- Databricks notebook source
SELECT parse_sql('
  MERGE INTO orders t
  USING updates s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET amount = s.amount
  WHEN NOT MATCHED THEN INSERT *
') AS parsed_sql;

-- COMMAND ----------

WITH parsed AS (
  SELECT parse_sql('
    MERGE INTO orders t
    USING updates s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET amount = s.amount
  ') AS result
)
SELECT
  get_json_object(result, '$[0].target_table_references') AS target_tables,
  get_json_object(result, '$[0].source_table_references') AS source_tables
FROM parsed;
