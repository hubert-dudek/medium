-- Databricks notebook source
-- Validate source access, March fiscal boundaries and current warehouse window support before creating views.
SET TIME ZONE 'UTC';

-- COMMAND ----------

USE CATALOG main;

-- COMMAND ----------

USE SCHEMA IDENTIFIER(:schema);

-- COMMAND ----------

SELECT current_user() AS deployment_identity, current_timezone() AS timezone;

-- COMMAND ----------

-- Analyze all columns the bundle depends on. No statement text or user identities are selected.
SELECT account_id, workspace_id, sku_name, cloud, usage_unit, usage_quantity,
       usage_start_time, usage_end_time, usage_date, record_type, ingestion_date,
       billing_origin_product, usage_metadata.warehouse_id, usage_metadata.cluster_id,
       usage_metadata.job_id, custom_tags
FROM system.billing.usage LIMIT 1;

-- COMMAND ----------

SELECT account_id, sku_name, cloud, usage_unit, currency_code, price_start_time,
       price_end_time, pricing.effective_list.default
FROM system.billing.list_prices LIMIT 1;

-- COMMAND ----------

SELECT account_id, workspace_id, warehouse_id, warehouse_name, warehouse_type,
       change_time, delete_time
FROM system.compute.warehouses LIMIT 1;

-- COMMAND ----------

SELECT account_id, workspace_id, compute.type, compute.warehouse_id, statement_id,
       start_time, execution_status, total_duration_ms, execution_duration_ms
FROM system.query.history LIMIT 1;

-- COMMAND ----------

-- Reject overlapping USD price histories: a metric dimension join must have at most one matching price.
-- The preceding MAX also catches overlaps with an earlier, longer interval, not just the previous row.
WITH checked AS (
  SELECT *, MAX(COALESCE(price_end_time, TIMESTAMP '9999-12-31 00:00:00')) OVER (
    PARTITION BY account_id, sku_name, cloud, usage_unit, currency_code
    ORDER BY price_start_time, COALESCE(price_end_time, TIMESTAMP '9999-12-31 00:00:00')
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
  ) AS prior_end
  FROM system.billing.list_prices WHERE currency_code = 'USD'
)
SELECT assert_true(COUNT_IF(
  price_start_time IS NULL OR price_end_time <= price_start_time OR price_start_time < prior_end
) = 0, 'Invalid/overlapping USD price intervals. Resolve pricing ambiguity before deploying.')
FROM checked;

-- COMMAND ----------

WITH cases(d, fy, fm, fq) AS (
  VALUES (DATE '2024-02-29', 2023, 12, 4), (DATE '2024-03-01', 2024, 1, 1),
         (DATE '2026-05-31', 2026, 3, 1), (DATE '2026-06-01', 2026, 4, 2),
         (DATE '2026-08-31', 2026, 6, 2), (DATE '2026-09-01', 2026, 7, 3),
         (DATE '2026-11-30', 2026, 9, 3), (DATE '2026-12-01', 2026, 10, 4),
         (DATE '2027-02-28', 2026, 12, 4), (DATE '2027-03-01', 2027, 1, 1)
)
SELECT assert_true(COUNT_IF(
  YEAR(ADD_MONTHS(d, -2)) != fy OR MONTH(ADD_MONTHS(d, -2)) != fm
  OR QUARTER(ADD_MONTHS(d, -2)) != fq
) = 0, 'March fiscal-calendar regression.') FROM cases;

-- COMMAND ----------

-- Session-local capability probe; no persistent object and no billing-table scan.
-- A failure here identifies a warehouse lacking date-offset support (18.1-equivalent engine required).
CREATE OR REPLACE TEMPORARY VIEW billing_offset_probe
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
source: |
  SELECT * FROM VALUES
    (DATE '2025-03-01', 100),
    (DATE '2026-02-01', 120),
    (DATE '2026-03-01', 150)
  AS t(month_start, amount)
dimensions:
  - name: month
    expr: source.month_start
  - name: fiscal_year_start
    expr: CAST(ADD_MONTHS(DATE_TRUNC('YEAR', ADD_MONTHS(month, -2)), 2) AS DATE)
measures:
  - name: spend
    expr: SUM(amount)
  - name: prior_month
    expr: SUM(amount)
    window:
      - order: month
        range: current
        semiadditive: last
        offset: -1 month
  - name: prior_year
    expr: SUM(amount)
    window:
      - order: month
        range: current
        semiadditive: last
        offset: -12 month
  - name: fiscal_ytd
    expr: SUM(amount)
    window:
      - order: month
        range: cumulative
        semiadditive: last
      - order: fiscal_year_start
        range: current
        semiadditive: last
$$;

-- COMMAND ----------

WITH results AS (
  SELECT month, MEASURE(prior_month) AS pm, MEASURE(prior_year) AS py,
         MEASURE(fiscal_ytd) AS fytd
  FROM billing_offset_probe GROUP BY month
)
SELECT assert_true(MAX(CASE WHEN month = DATE '2026-03-01'
  THEN CASE WHEN pm = 120 AND py = 100 AND fytd = 150 THEN 1 ELSE 0 END END) = 1,
  'Offset/fiscal-window capability probe failed.') FROM results;

-- COMMAND ----------

DROP VIEW IF EXISTS billing_offset_probe;
