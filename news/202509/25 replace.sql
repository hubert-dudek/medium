-- Databricks notebook source
DROP TABLE IF EXISTS students;
DROP TABLE IF EXISTS contacts;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS new_events;

-- COMMAND ----------

CREATE TABLE contacts(email STRING, state STRING);

INSERT INTO contacts
  VALUES
    ('JOE@EXAMPLE.COM', 'Old'),
    ('patrick@example.com', 'Old'),
    (NULL, 'Old');
    
SELECT * FROM contacts;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW contacts_fixes AS
SELECT
  *
FROM
  VALUES 
  ('joe@example.com', 'Replacement'), -- will replace JOE@EXAMPLE.COM
  ('jane@example.com', 'New'), -- will be added as new records
  (NULL, 'Replacement') -- will replace NULL we use <=> operator save for NULLs
  AS s (email, state);

  SELECT * FROM contacts_fixes;

-- COMMAND ----------

INSERT INTO
  TABLE contacts AS target -- table in which we do replacement
  REPLACE ON lower(trim(target.email)) <=> lower(trim(source.email)) (
    -- <=> spaship operator save for NULL. We can use functions in an expression
    SELECT
      *
    FROM
      contacts_fixes -- SELECT statement to overwrite replacements
  ) AS source;

-- COMMAND ----------

SELECT
  *
FROM
  contacts;

-- COMMAND ----------

CREATE TABLE events (date DATE, payload STRING);
CREATE TABLE new_events (date DATE, payload STRING);

-- COMMAND ----------

INSERT INTO events VALUES 
(DATE'2025-08-10','old1'), 
(DATE'2025-08-20','old2'), 
(DATE'2025-08-30','KEEP'); -- just for perposes of this exercise we mark record which will not be deleted as KEEP

SELECT * FROM events;

-- COMMAND ----------

INSERT INTO new_events VALUES 
(DATE'2025-08-25','new');  -- we’re inserting newer data, all older than this records will be deleted

SELECT * FROM new_events;

-- COMMAND ----------

INSERT INTO TABLE events AS t
REPLACE ON t.date < s.date
(SELECT * FROM new_events) AS s;

-- COMMAND ----------

SELECT * FROM events ORDER BY date;

-- COMMAND ----------

CREATE TABLE students (name STRING, country STRING)
PARTITIONED BY (country)
LOCATION "abfss://ucmain@uchubmain.dfs.core.windows.net/students";

-- Insert initial data into partitions US, UK, IT, DE, CN
INSERT INTO students VALUES
('Dylan', 'US'),
('Doug', 'UK'),
('Julia', 'IT');

SELECT * FROM students;

-- COMMAND ----------

-- New data to replace UK partition and add FR partition
CREATE OR REPLACE TEMP VIEW new_students AS SELECT * FROM VALUES
('Jennie', 'UK'), -- will replace the UK partition data
('Peter', 'FR') AS s(name, country); -- will create a new FR partition

SELECT * FROM new_students;

-- COMMAND ----------

DESCRIBE EXTENDED students;

-- COMMAND ----------

--validate partitions to replace
SELECT country FROM new_students;

-- COMMAND ----------

-- Perform dynamic partition overwrite using the country key
INSERT INTO TABLE students
REPLACE USING (country)
SELECT * FROM new_students;

-- COMMAND ----------

SELECT * FROM students;
