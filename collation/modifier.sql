-- Databricks notebook source
-- MAGIC %md
-- MAGIC | Modifier | Description (Databricks SQL 16.2+)                                                                 |
-- MAGIC |----------|-----------------------------------------------------------------------------------------------------|
-- MAGIC | **CS**   | Case-Sensitive (default) – e.g., `'A'` ≠ `'a'`                 |
-- MAGIC | **CI**   | Case-Insensitive – e.g., `'A'` = `'a'`                         |
-- MAGIC | **AS**   | Accent-Sensitive (default) – e.g., `'e'` ≠ `'é'`               |
-- MAGIC | **AI**   | Accent-Insensitive – e.g., `'e'` = `'é'`                       |
-- MAGIC | **RTRIM**| Trailing space insensitive – ignores trailing spaces (new in 16.2)  |
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE my_table(name STRING);

-- COMMAND ----------

INSERT INTO
  my_table (name)
VALUES
  ('JOHN'),
  ('john'),
  ('John'),
  ('Peña'),
  ('Hello '),
  ('Hello'),
  ('apple'),
  ('Banana'),
  ('e'),
  ('é');

-- COMMAND ----------

SELECT name FROM collations()

-- COMMAND ----------

SELECT
  name
FROM
  my_table
WHERE
  name COLLATE UNICODE_CI = 'john';

-- COMMAND ----------

SELECT
  *
FROM
  my_table
WHERE
  name COLLATE UNICODE_CI = 'Pena';

-- COMMAND ----------

SELECT
  *
FROM
  my_table
WHERE
  name COLLATE UNICODE_RTRIM = 'Hello';

-- COMMAND ----------

CREATE OR REPLACE TABLE my_table_ci(name STRING COLLATE UNICODE_CI_AI); 
INSERT INTO my_table_ci SELECT name FROM my_table;

SELECT
  name COLLATE UNICODE_CI_AI
FROM
  my_table_ci
ORDER BY
  name COLLATE UNICODE_CI_AI;

-- COMMAND ----------

SELECT
  name COLLATE cs_AI
FROM
  my_table
ORDER BY
  name COLLATE cs_AI;

-- COMMAND ----------

SELECT
  name
FROM
  my_table
ORDER BY
  name COLLATE UNICODE_RTRIM;
