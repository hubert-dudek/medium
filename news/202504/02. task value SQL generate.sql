-- Databricks notebook source
-- MAGIC %md
-- MAGIC https://docs.databricks.com/aws/en/jobs/dynamic-value-references

-- COMMAND ----------

-- MAGIC %md
-- MAGIC {{tasks.<task_name>.output.rows}}
-- MAGIC
-- MAGIC The output rows of an upstream SQL task <task_name>. If passed as the Inputs for a For each task, each row is sent iteratively to the nested task. SQL output is limited to 1,000 rows, and 48 KB in size. See SQL output options.
-- MAGIC
-- MAGIC {{tasks.<task_name>.output.first_row}}
-- MAGIC
-- MAGIC The first row of the output of an upstream SQL task <task_name>. SQL output is limited to 1,000 rows and 48 KB in size.
-- MAGIC
-- MAGIC {{tasks.<task_name>.output.first_row.<column_alias>}}
-- MAGIC
-- MAGIC The value of the column <column_alias> in the first row of the output of an upstream SQL task <task_name>. SQL output is limited to 1,000 rows and 48 KB in size.
-- MAGIC
-- MAGIC {{tasks.<task_name>.output.alert_state}}
-- MAGIC
-- MAGIC The state of an upstream SQL alert task. The value is one of UNKNOWN, OK, or TRIGGERED.

-- COMMAND ----------

CREATE OR REPLACE TABLE example_table (
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  sale INT
);

INSERT INTO example_table (sale) VALUES (10), (20), (30);

SELECT id
FROM example_table;
