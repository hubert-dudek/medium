-- Databricks notebook source
WITH orders (order_id, status_code) AS (
  VALUES
    (101, 1),
    (102, 2),
    (103, 4)
)
SELECT 
  order_id,
  CASE status_code
    WHEN 1 THEN 'New'
    WHEN 2 THEN 'Shipped'
    WHEN 3 THEN 'Delivered'
    ELSE 'Unknown'
  END AS status_label
FROM orders;


-- COMMAND ----------

WITH results (student, score) AS (
  VALUES
    ('Alice', 85),
    ('Bob', 70),
    ('Charlie', 45)
)
SELECT
  student,
  CASE
    WHEN score >= 80 THEN 'High'
    WHEN score >= 60 THEN 'Medium'
    ELSE 'Low'
  END AS performance
FROM results;


-- COMMAND ----------

BEGIN
  DECLARE choice INT DEFAULT 3;
  DECLARE result STRING;

  CASE choice
    WHEN 1 THEN
      VALUES ('one fish');
    WHEN 2 THEN
      VALUES ('two fish');
    WHEN 3 THEN
      VALUES ('red fish');
    WHEN 4 THEN
      VALUES ('blue fish');
    ELSE
      VALUES ('no fish');
  END CASE;

END;

