-- Databricks notebook source
-- Create a new table with row tracking
CREATE OR REPLACE TABLE my_table(id INT, name STRING, value DOUBLE) USING DELTA
TBLPROPERTIES (delta.enableRowTracking = true);

-- Or, alter an existing table
ALTER TABLE
  my_table
SET TBLPROPERTIES
  (delta.enableRowTracking = true);

INSERT INTO
  my_table (id, name, value)
VALUES (1, 'Alice', 10.5);

-- COMMAND ----------

SELECT
  id,
  name,
  value,
  _metadata.row_id AS unique_row_id,
  _metadata.row_commit_version AS commit_version
FROM
  my_table;

-- COMMAND ----------

-- Example insert
INSERT INTO
  my_table
VALUES (2, 'Bob', 20.0);

-- View row IDs
SELECT
  id,
  _metadata.row_id AS unique_row_id
FROM
  my_table;

-- COMMAND ----------

UPDATE my_table SET name = 'Bob' Where id = 1;

-- View commit version
SELECT
  id,
  _metadata.row_commit_version AS commit_version
FROM
  my_table
Where id = 1;

-- COMMAND ----------

-- Suppose we have a materialized view that filters or aggregates our main table
CREATE OR REPLACE MATERIALIZED VIEW mv_my_table AS
SELECT
  id,
  sum(value) AS total_value
FROM
  my_table
GROUP BY
  id;

-- COMMAND ----------

-- Let's assume we last processed version 7
SELECT
  id,
  name,
  value,
  _metadata.row_commit_version AS commit_version
FROM
  my_table
WHERE
  _metadata.row_commit_version = 8;

-- COMMAND ----------

-- We have source table: my_table (with row tracking ON)
-- Create a destination table that references the source row ID
CREATE OR REPLACE TABLE my_table_cleaned (original_row_id STRING, id INT, name STRING, cleaned_value DOUBLE)
USING DELTA
TBLPROPERTIES (delta.enableRowTracking = true);

-- COMMAND ----------

-- Insert from my_table into my_table_cleaned, capturing row ID
INSERT INTO
  my_table_cleaned
SELECT
  _metadata.row_id AS original_row_id,
  id,
  name,
  value * 0.9 AS cleaned_value -- e.g. some transformation
FROM
  my_table;

-- COMMAND ----------

-- Combine both tables to trace a row’s lineage
SELECT
  s._metadata.row_id AS source_row_id,
  c._metadata.row_id AS cleaned_row_id,
  s.name,
  s.value,
  c.cleaned_value
FROM
  my_table AS s
    JOIN my_table_cleaned AS c
      ON s._metadata.row_id = c.original_row_id
