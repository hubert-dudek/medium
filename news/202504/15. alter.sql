-- Databricks notebook source
-- MAGIC %md
-- MAGIC Edit multiple columns using ALTER TABLE: You can now alter multiple columns in a single ALTER TABLE statement. See ALTER TABLE … COLUMN clause.

-- COMMAND ----------

CREATE OR REPLACE TABLE unstable_table (
    id INT,
    will_be_commented STRING,
    will_be_bool STRING,
    will_be_num STRING,
    will_be_smth STRING
)  
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

-- COMMAND ----------

ALTER TABLE 
   unstable_table 
ALTER COLUMN
   will_be_commented COMMENT 'boolean column',
   will_be_bool AFTER id,
   will_be_num AFTER will_be_bool,
   will_be_smth SET DEFAULT 'default_value';
