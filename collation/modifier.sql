-- Databricks notebook source
-- MAGIC %md
-- MAGIC | Modifier | Description                                                            |
-- MAGIC |----------|-----------------------------------------------------------------------------------------------------|
-- MAGIC | **CS**   | Case-Sensitive (default) – e.g., `'A'` ≠ `'a'`                 |
-- MAGIC | **CI**   | Case-Insensitive – e.g., `'A'` = `'a'`                         |
-- MAGIC | **AS**   | Accent-Sensitive (default) – e.g., `'e'` ≠ `'é'`               |
-- MAGIC | **AI**   | Accent-Insensitive – e.g., `'e'` = `'é'`                       |
-- MAGIC | **RTRIM**| Trailing space insensitive – ignores trailing spaces   |
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
  ('Pena'),
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
  name COLLATE UNICODE_AI = 'Pena';

-- COMMAND ----------

SELECT
  *
FROM
  my_table
WHERE
  name COLLATE UNICODE_RTRIM = 'Hello';

-- COMMAND ----------

SELECT
  name
FROM
  my_table
ORDER BY
  name;

-- COMMAND ----------

SELECT
  name
FROM
  my_table
ORDER BY
  name COLLATE UNICODE_CI_AI;
