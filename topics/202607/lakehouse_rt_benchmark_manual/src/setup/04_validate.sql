-- Databricks notebook source
-- Returns six rows: one expected/actual count comparison per generated table.

WITH checks AS (
  SELECT 'dim_date' AS table_name, CAST(1461 AS BIGINT) AS expected_rows, COUNT(*) AS actual_rows
  FROM IDENTIFIER(:catalog || '.' || :schema || '.dim_date')
  UNION ALL
  SELECT 'dim_category', CAST(100 AS BIGINT), COUNT(*)
  FROM IDENTIFIER(:catalog || '.' || :schema || '.dim_category')
  UNION ALL
  SELECT 'dim_customer', CAST(:customer_rows AS BIGINT), COUNT(*)
  FROM IDENTIFIER(:catalog || '.' || :schema || '.dim_customer')
  UNION ALL
  SELECT 'dim_merchant', CAST(:merchant_rows AS BIGINT), COUNT(*)
  FROM IDENTIFIER(:catalog || '.' || :schema || '.dim_merchant')
  UNION ALL
  SELECT 'dim_product', CAST(:product_rows AS BIGINT), COUNT(*)
  FROM IDENTIFIER(:catalog || '.' || :schema || '.dim_product')
  UNION ALL
  SELECT 'transactions', CAST(:transaction_rows AS BIGINT), COUNT(*)
  FROM IDENTIFIER(:catalog || '.' || :schema || '.transactions')
)
SELECT
  CAST(:scale_name AS STRING) AS scale_name,
  table_name,
  expected_rows,
  actual_rows,
  CASE WHEN expected_rows = actual_rows THEN 'OK' ELSE 'MISMATCH' END AS validation_status
FROM checks
ORDER BY table_name;

