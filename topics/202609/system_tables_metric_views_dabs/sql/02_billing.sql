-- Databricks notebook source
-- Define the canonical billing calculation once, with time-valid prices and March fiscal dimensions.
-- Explicit decimal widths preserve sub-cent prices without converting currency to binary floating point.
-- SQL-only deployment notebook. The schema parameter comes from the native DAB schema resource.
SET TIME ZONE 'UTC';

-- COMMAND ----------

CREATE OR REPLACE VIEW IDENTIFIER('main.' || :schema || '.billing')
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: Account-wide retained billing usage at effective public list prices in USD not invoices. Fiscal year starts
  March 1. Signed corrections are included. Cost is NULL when a nonzero usage record cannot be priced.
source: system.billing.usage
joins:
- name: prices
  source: |
    SELECT account_id, sku_name, cloud, usage_unit, price_start_time, price_end_time,
           CAST(pricing.effective_list.default AS DECIMAL(20,12)) AS unit_price_usd
    FROM system.billing.list_prices
    WHERE currency_code = 'USD'
  'on': |
    source.account_id = prices.account_id
    AND source.sku_name = prices.sku_name
    AND source.cloud = prices.cloud
    AND source.usage_unit = prices.usage_unit
    AND source.usage_start_time >= prices.price_start_time
    AND (prices.price_end_time IS NULL OR source.usage_start_time < prices.price_end_time)
    AND (prices.price_end_time IS NULL OR source.usage_end_time <= prices.price_end_time)
- name: warehouses
  source: main.system_tables_metrics.warehouse_latest
  'on': |
    source.account_id = warehouses.account_id
    AND source.workspace_id = warehouses.workspace_id
    AND source.usage_metadata.warehouse_id = warehouses.warehouse_id
dimensions:
- name: usage_date
  expr: source.usage_date
  display_name: Usage Date
  comment: UTC usage date prefer start, end ranges.
- name: usage_month
  expr: CAST(DATE_TRUNC('MONTH', usage_date) AS DATE)
  display_name: Usage Month
  comment: Calendar month used as the time-series anchor.
- name: fiscal_year_start
  expr: CAST(ADD_MONTHS(DATE_TRUNC('YEAR', ADD_MONTHS(usage_date, -2)), 2) AS DATE)
  display_name: Fiscal Year Start
  comment: March 1 of the fiscal start year.
- name: fiscal_year
  expr: YEAR(fiscal_year_start)
  display_name: Fiscal Year
  comment: 'Fiscal START year: FY2026/27 starts on 2026-03-01.'
- name: fiscal_year_label
  expr: CONCAT('FY', fiscal_year, '/', RIGHT(CAST(fiscal_year + 1 AS STRING), 2))
  display_name: Fiscal Year Label
- name: fiscal_month_number
  expr: MONTH(ADD_MONTHS(usage_date, -2))
  display_name: Fiscal Month Number
  comment: March=1, February=12.
- name: fiscal_quarter
  expr: CONCAT('Q', QUARTER(ADD_MONTHS(usage_date, -2)))
  display_name: Fiscal Quarter
  comment: Q1 Mar-May Q2 Jun-Aug Q3 Sep-Nov Q4 Dec-Feb. Always pair with fiscal_year.
- name: fiscal_quarter_start
  expr: CAST(ADD_MONTHS(DATE_TRUNC('QUARTER', ADD_MONTHS(usage_date, -2)), 2) AS DATE)
  display_name: Fiscal Quarter Start
- name: account_id
  expr: source.account_id
  display_name: Account Id
- name: workspace_id
  expr: source.workspace_id
  display_name: Workspace Id
- name: sku_name
  expr: source.sku_name
  display_name: Sku Name
- name: cloud
  expr: source.cloud
  display_name: Cloud
- name: usage_unit
  expr: source.usage_unit
  display_name: Usage Unit
  comment: Do not add quantities across different usage units.
- name: billing_origin_product
  expr: source.billing_origin_product
  display_name: Billing Origin Product
- name: warehouse_id
  expr: source.usage_metadata.warehouse_id
  display_name: Warehouse Id
- name: warehouse_name
  expr: warehouses.warehouse_name
  display_name: Warehouse Name
  comment: Latest observed warehouse name NULL for non-warehouse/unmatched records.
- name: cluster_id
  expr: source.usage_metadata.cluster_id
  display_name: Cluster Id
- name: job_id
  expr: source.usage_metadata.job_id
  display_name: Job Id
- name: environment
  expr: COALESCE(element_at(source.custom_tags, 'environment'), element_at(source.custom_tags, 'env'), '(untagged)')
  display_name: Environment
- name: cost_center
  expr: COALESCE(element_at(source.custom_tags, 'cost_center'), '(untagged)')
  display_name: Cost Center
- name: record_type
  expr: source.record_type
  display_name: Record Type
  comment: ORIGINAL, RETRACTION and RESTATEMENT do not filter these out of spend.
- name: price_status
  expr: CASE WHEN source.usage_quantity = 0 OR prices.unit_price_usd IS NOT NULL THEN 'priced' ELSE 'unpriced' END
  display_name: Price Status
  comment: unpriced includes missing USD prices and records spanning price changes.
measures:
- name: record_count
  expr: COUNT(1)
  display_name: Record Count
  comment: Physical billing records, including correction records not a customer count.
- name: unpriced_record_count
  expr: SUM(CASE WHEN source.usage_quantity <> 0 AND prices.unit_price_usd IS NULL THEN 1 ELSE 0 END)
  display_name: Unpriced Record Count
  comment: Nonzero usage records with no unambiguous full-interval USD price. Inspect before using any cost.
- name: priced_cost_usd
  expr: SUM(CASE WHEN source.usage_quantity = 0 THEN 0
    ELSE CAST(source.usage_quantity AS DECIMAL(28,9)) * prices.unit_price_usd END)
  display_name: Priced Cost Usd
  comment: Subtotal for matched prices only. Not a complete spend total when unpriced_record_count > 0.
- name: spend_usd
  expr: CASE WHEN MEASURE(unpriced_record_count) = 0 THEN MEASURE(priced_cost_usd) END
  display_name: Spend Usd
  comment: Estimated effective public-list-price consumption in USD, including signed corrections. NULL if incomplete
    no discounts, tax, credits or external cloud infrastructure.
  synonyms:
  - estimated spend
  - total consumption
  - list price cost
- name: dbu_usage
  expr: SUM(source.usage_quantity) FILTER (WHERE source.usage_unit = 'DBU')
  display_name: Dbu Usage
  comment: Signed DBUs only all non-DBU units excluded. Not a monetary amount.
- name: usage_quantity_single_unit
  expr: CASE WHEN COUNT(DISTINCT source.usage_unit) = 1 THEN SUM(source.usage_quantity) END
  display_name: Usage Quantity Single Unit
  comment: Quantity only when exactly one usage unit is present. Group by usage_unit.
- name: first_usage_date
  expr: MIN(source.usage_date)
  display_name: First Usage Date
  comment: Earliest retained usage in the selected population not proof of continuous history.
- name: last_usage_date
  expr: MAX(source.usage_date)
  display_name: Last Usage Date
  comment: Latest observed usage date late records can still arrive.
- name: last_ingestion_date
  expr: MAX(source.ingestion_date)
  display_name: Last Ingestion Date
  comment: Latest ingestion date observed for the selected usage not a completeness watermark.
$$;