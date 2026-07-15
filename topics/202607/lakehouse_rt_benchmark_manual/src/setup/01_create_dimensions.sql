-- Databricks notebook source
-- Creates deterministic Unity Catalog managed dimension tables.
-- This notebook intentionally contains DDL and runs only on the standard prep warehouse.

CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema || '.dim_date')
USING DELTA
CLUSTER BY (calendar_date)
AS
SELECT
  CAST(id + 1 AS INT) AS date_id,
  DATE_ADD(DATE '2022-01-01', CAST(id AS INT)) AS calendar_date,
  EXTRACT(YEAR FROM DATE_ADD(DATE '2022-01-01', CAST(id AS INT))) AS calendar_year,
  EXTRACT(MONTH FROM DATE_ADD(DATE '2022-01-01', CAST(id AS INT))) AS calendar_month,
  EXTRACT(DAY FROM DATE_ADD(DATE '2022-01-01', CAST(id AS INT))) AS calendar_day,
  WEEKOFYEAR(DATE_ADD(DATE '2022-01-01', CAST(id AS INT))) AS calendar_week,
  DAYOFWEEK(DATE_ADD(DATE '2022-01-01', CAST(id AS INT))) AS day_of_week,
  CASE WHEN DAYOFWEEK(DATE_ADD(DATE '2022-01-01', CAST(id AS INT))) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM RANGE(0, 1461, 1, 1);

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema || '.dim_category')
USING DELTA
CLUSTER BY (category_id)
AS
SELECT
  CAST(id + 1 AS INT) AS category_id,
  CONCAT('CATEGORY_', LPAD(CAST(id + 1 AS STRING), 3, '0')) AS category_name,
  CASE CAST(PMOD(id, 8) AS INT)
    WHEN 0 THEN 'GROCERY'
    WHEN 1 THEN 'ELECTRONICS'
    WHEN 2 THEN 'TRAVEL'
    WHEN 3 THEN 'FASHION'
    WHEN 4 THEN 'HOME'
    WHEN 5 THEN 'HEALTH'
    WHEN 6 THEN 'SERVICES'
    ELSE 'OTHER'
  END AS category_group
FROM RANGE(0, 100, 1, 1);

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema || '.dim_customer')
USING DELTA
CLUSTER BY (customer_id, country_code)
AS
SELECT
  CAST(id + 1 AS BIGINT) AS customer_id,
  CONCAT('CUS_', LPAD(CAST(id + 1 AS STRING), 12, '0')) AS customer_key,
  CONCAT('C', LPAD(CAST(PMOD(XXHASH64(id, 101), 20) AS STRING), 2, '0')) AS country_code,
  CAST(PMOD(XXHASH64(id, 103), 8) + 1 AS INT) AS region_id,
  CASE CAST(PMOD(XXHASH64(id, 107), 4) AS INT)
    WHEN 0 THEN 'CONSUMER'
    WHEN 1 THEN 'SMB'
    WHEN 2 THEN 'MIDMARKET'
    ELSE 'ENTERPRISE'
  END AS customer_segment,
  DATE_ADD(DATE '2018-01-01', CAST(PMOD(XXHASH64(id, 109), 1461) AS INT)) AS signup_date,
  CASE CAST(PMOD(XXHASH64(id, 113), 5) AS INT)
    WHEN 0 THEN 'VERY_LOW'
    WHEN 1 THEN 'LOW'
    WHEN 2 THEN 'MEDIUM'
    WHEN 3 THEN 'HIGH'
    ELSE 'VERY_HIGH'
  END AS risk_band,
  CONCAT('Customer ', CAST(id + 1 AS STRING)) AS customer_name
FROM RANGE(0, CAST(:customer_rows AS BIGINT), 1, CAST(:customer_num_parts AS INT));

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema || '.dim_merchant')
USING DELTA
CLUSTER BY (merchant_id, country_code)
AS
SELECT
  CAST(id + 1 AS BIGINT) AS merchant_id,
  CONCAT('MER_', LPAD(CAST(id + 1 AS STRING), 12, '0')) AS merchant_key,
  CAST(PMOD(XXHASH64(id, 127), 100) + 1 AS INT) AS category_id,
  CONCAT('C', LPAD(CAST(PMOD(XXHASH64(id, 131), 20) AS STRING), 2, '0')) AS country_code,
  CASE CAST(PMOD(XXHASH64(id, 137), 4) AS INT)
    WHEN 0 THEN 'LOW'
    WHEN 1 THEN 'MEDIUM'
    WHEN 2 THEN 'HIGH'
    ELSE 'VERY_HIGH'
  END AS merchant_risk_band,
  CONCAT('Merchant ', CAST(id + 1 AS STRING)) AS merchant_name
FROM RANGE(0, CAST(:merchant_rows AS BIGINT), 1, CAST(:merchant_num_parts AS INT));

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema || '.dim_product')
USING DELTA
CLUSTER BY (product_id, category_id)
AS
SELECT
  CAST(id + 1 AS BIGINT) AS product_id,
  CONCAT('PRD_', LPAD(CAST(id + 1 AS STRING), 12, '0')) AS product_key,
  CAST(PMOD(XXHASH64(id, 139), 100) + 1 AS INT) AS category_id,
  CAST(PMOD(XXHASH64(id, 149), 5000) + 1 AS INT) AS brand_id,
  CAST((PMOD(XXHASH64(id, 151), 200000) + 100) / 100.0 AS DECIMAL(18, 2)) AS unit_cost,
  CONCAT('Product ', CAST(id + 1 AS STRING)) AS product_name
FROM RANGE(0, CAST(:product_rows AS BIGINT), 1, CAST(:product_num_parts AS INT));

