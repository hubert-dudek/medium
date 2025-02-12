-- Databricks notebook source
-- Table without explicit collation (uses default UTF8_BINARY, case-sensitive)
CREATE OR REPLACE TABLE default_names (
  name STRING
) USING DELTA;

INSERT INTO default_names VALUES ('Alice'), ('alex'), ('ALICE');

-- Table with case-insensitive collation for the name column
CREATE OR REPLACE TABLE ci_names (
  name STRING COLLATE 'EN_CI'  -- English locale, Case-Insensitive
) USING DELTA;

INSERT INTO ci_names VALUES ('Alice'), ('alex'), ('ALICE');

-- COMMAND ----------

-- Case-sensitive search (default collation): only exact-case matches
SELECT * 
FROM default_names 
WHERE name = 'alice';
-- Returns only "alex" (exact lowercase match), does NOT return "Alice" or "ALICE"

-- COMMAND ----------

-- Case-insensitive search (collation EN_CI): matches regardless of case
SELECT * 
FROM ci_names 
WHERE name = 'alice';
-- Returns "Alice", "alex", and "ALICE" because collation ignores case differences

-- COMMAND ----------

SELECT * FROM ci_names ORDER BY name;
-- The result will treat "Alice", "alex", "ALICE" as equivalent in sort order, 
-- effectively sorting without regard to case.

-- COMMAND ----------

-- Table with default collation (binary) for Greek names
CREATE OR REPLACE TABLE greek_names_default (
  name STRING
) USING DELTA;

-- Same data with a Greek locale collation, accent-insensitive
CREATE OR REPLACE TABLE greek_names_collated (
  name STRING COLLATE 'EL_AI'  -- Greek locale, Accent-Insensitive
) USING DELTA;

-- Insert some Greek names (with and without accent marks)
INSERT INTO greek_names_default VALUES 
  ('αλφα'),   -- "alpha" in lowercase (no accent)
  ('άλφα'),   -- "alpha" with accent on first letter
  ('Beta'),   -- including an English word to show ordering differences
  ('βήτα');   -- "beta" in Greek with accent

INSERT INTO greek_names_collated VALUES 
  ('αλφα'),
  ('άλφα'),
  ('Beta'),
  ('βήτα');


-- COMMAND ----------

-- Ordering without Greek collation (might be off due to binary comparison)
SELECT name FROM greek_names_default ORDER BY name;

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

