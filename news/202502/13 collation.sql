-- Databricks notebook source
-- Table without explicit collation (uses default UTF8_BINARY, case-sensitive)
CREATE OR REPLACE TABLE default_names (
  name STRING
);

INSERT INTO default_names VALUES ('Alice'), ('ALICE');

-- Table with case-insensitive collation for the name column
CREATE OR REPLACE TABLE ci_names (
  name STRING COLLATE EN_CI  -- English locale, Case-Insensitive
);

INSERT INTO ci_names VALUES ('Alice'), ('ALICE');

-- COMMAND ----------

-- Case-sensitive search (default collation): only exact-case matches
SELECT * 
FROM default_names 
WHERE name = 'Alice';

-- COMMAND ----------

-- Case-insensitive search (collation EN_CI): matches regardless of case
SELECT * 
FROM ci_names 
WHERE name = 'alice';

-- COMMAND ----------

-- Same data with a Greek locale collation, accent-insensitive
CREATE OR REPLACE TABLE greek_names_collated (
  name STRING COLLATE EL_AI  -- Greek locale, Accent-Insensitive
);

INSERT INTO greek_names_collated VALUES 
  ('αλφα'),
  ('άλφα'),
  ('βήτα');


-- COMMAND ----------

-- Ordering with Greek collation (EL_AI)
SELECT name FROM greek_names_collated ORDER BY name;

-- COMMAND ----------

-- Accent-insensitive filtering on Greek collation
SELECT * 
FROM greek_names_collated 
WHERE name = 'αλφα';
-- Returns both 'αλφα' and 'άλφα' since the collation ignores the accent mark.


-- COMMAND ----------

ANALYZE TABLE greek_names_collated COMPUTE DELTA STATISTICS;
