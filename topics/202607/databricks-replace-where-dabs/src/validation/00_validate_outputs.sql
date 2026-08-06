-- Databricks notebook source
-- Lightweight post-deployment checks. These queries intentionally expose rows
-- rather than hiding them behind a pass/fail framework, making the demos easy to inspect.

USE CATALOG IDENTIFIER(:output_catalog);
USE SCHEMA IDENTIFIER(:output_schema);

-- COMMAND ----------
SELECT '01 sales_by_product_spec' AS dataset, COUNT(*) AS row_count
FROM sales_by_product_spec
UNION ALL
SELECT '02 invoice_vat_calculation', COUNT(*) FROM invoice_vat_calculation
UNION ALL
SELECT '03 weather_forecast_current', COUNT(*) FROM weather_forecast_current
UNION ALL
SELECT '04 customer_daily_metrics', COUNT(*) FROM customer_daily_metrics
UNION ALL
SELECT '05 invoice_reporting_open_period', COUNT(*) FROM invoice_reporting_open_period
UNION ALL
SELECT '06 settlement_canonical', COUNT(*) FROM settlement_canonical
ORDER BY dataset;

-- COMMAND ----------
-- Use case 1: after demo_01, product 1 should show a newer spec only in the recent window.
SELECT order_date, product_id, product_name, spec_version, ram_gb, energy_class, net_revenue
FROM sales_by_product_spec
WHERE product_id = 1
ORDER BY order_date DESC;

-- COMMAND ----------
-- Use case 2: December should retain the old rule while dates from 1 January use the corrected rule.
SELECT invoice_date, invoice_id, net_amount, vat_rate, vat_amount, gross_amount, rule_version
FROM invoice_vat_calculation
ORDER BY invoice_date, invoice_id;

-- COMMAND ----------
SELECT location_id, forecast_date, issued_at, condition, temperature_c, model_name, confidence
FROM weather_forecast_current
ORDER BY location_id, forecast_date;

-- COMMAND ----------
SELECT metric_date, customer_id, event_count, purchase_count, net_revenue, average_quality_score
FROM customer_daily_metrics
WHERE metric_date BETWEEN DATE_SUB(CURRENT_DATE(), 48) AND DATE_SUB(CURRENT_DATE(), 35)
ORDER BY metric_date, customer_id;

-- COMMAND ----------
-- Compare source and target to see that the June correction stays frozen while July changes.
SELECT accounting_date, invoice_id, cost_center, allocation_version, commission_pct, amount_eur, commission_eur
FROM invoice_reporting_open_period
WHERE legal_entity = 'PL01'
ORDER BY accounting_date;

-- COMMAND ----------
SELECT provider, business_date, transaction_id, fee_amount, source_file_version, source_file_checksum
FROM settlement_canonical
WHERE provider = 'PAYFAST'
ORDER BY business_date, transaction_id;

-- COMMAND ----------
-- Backfill request audit trail.
SELECT *
FROM IDENTIFIER(:source_catalog || '.' || :source_schema || '.backfill_requests')
ORDER BY requested_at DESC;
