-- Databricks notebook source
-- Materialize a semantic mirror of canonical billing; Databricks creates and manages its own Lakeflow pipeline.
-- SQL-only deployment notebook. The schema parameter comes from the native DAB schema resource.
SET TIME ZONE 'UTC';

-- COMMAND ----------

CREATE OR REPLACE VIEW IDENTIFIER('main.' || :schema || '.billing')
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: Account-wide retained billing usage at effective public list prices in USD, not invoices. Fiscal year starts
  March 1. Signed corrections are included. Cost is NULL when a nonzero usage record cannot be priced. Managed metric-view
  materialization query freshness can lag.
source: main.system_tables_metrics.billing
dimensions:
- name: usage_date
  expr: source.usage_date
  display_name: Usage Date
  comment: UTC usage date, prefer start to end ranges.
- name: usage_month
  expr: source.usage_month
  display_name: Usage Month
  comment: Calendar month used as the time-series anchor.
- name: fiscal_year_start
  expr: source.fiscal_year_start
  display_name: Fiscal Year Start
  comment: March 1 of the fiscal start year.
- name: fiscal_year
  expr: source.fiscal_year
  display_name: Fiscal Year
  comment: 'Fiscal START year: FY2026/27 starts on 2026-03-01.'
- name: fiscal_year_label
  expr: source.fiscal_year_label
  display_name: Fiscal Year Label
- name: fiscal_month_number
  expr: source.fiscal_month_number
  display_name: Fiscal Month Number
  comment: March=1, February=12.
- name: fiscal_quarter
  expr: source.fiscal_quarter
  display_name: Fiscal Quarter
  comment: Q1 Mar-May, Q2 Jun-Aug, Q3 Sep-Nov, Q4 Dec-Feb. Always pair with fiscal_year.
- name: fiscal_quarter_start
  expr: source.fiscal_quarter_start
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
  expr: source.warehouse_id
  display_name: Warehouse Id
- name: warehouse_name
  expr: source.warehouse_name
  display_name: Warehouse Name
  comment: Latest observed warehouse name, NULL for non-warehouse/unmatched records.
- name: cluster_id
  expr: source.cluster_id
  display_name: Cluster Id
- name: job_id
  expr: source.job_id
  display_name: Job Id
- name: environment
  expr: source.environment
  display_name: Environment
- name: cost_center
  expr: source.cost_center
  display_name: Cost Center
- name: record_type
  expr: source.record_type
  display_name: Record Type
  comment: ORIGINAL, RETRACTION and RESTATEMENT, do not filter these out of spend.
- name: price_status
  expr: source.price_status
  display_name: Price Status
  comment: unpriced includes missing USD prices and records spanning price changes.
measures:
- name: record_count
  expr: MEASURE(source.record_count)
  display_name: Record Count
  comment: Physical billing records, including correction records, not a customer count.
- name: unpriced_record_count
  expr: MEASURE(source.unpriced_record_count)
  display_name: Unpriced Record Count
  comment: Nonzero usage records with no unambiguous full-interval USD price. Inspect before using any cost.
- name: priced_cost_usd
  expr: MEASURE(source.priced_cost_usd)
  display_name: Priced Cost Usd
  comment: Subtotal for matched prices only. Not a complete spend total when unpriced_record_count > 0.
- name: spend_usd
  expr: MEASURE(source.spend_usd)
  display_name: Spend Usd
  comment: Estimated effective public-list-price consumption in USD, including signed corrections. NULL if incomplete,
    no discounts, tax, credits or external cloud infrastructure.
  synonyms:
  - estimated spend
  - total consumption
  - list price cost
- name: dbu_usage
  expr: MEASURE(source.dbu_usage)
  display_name: Dbu Usage
  comment: Signed DBUs only, all non-DBU units excluded. Not a monetary amount.
- name: usage_quantity_single_unit
  expr: MEASURE(source.usage_quantity_single_unit)
  display_name: Usage Quantity Single Unit
  comment: Quantity only when exactly one usage unit is present. Group by usage_unit.
- name: first_usage_date
  expr: MEASURE(source.first_usage_date)
  display_name: First Usage Date
  comment: Earliest retained usage in the selected population, not proof of continuous history.
- name: last_usage_date
  expr: MEASURE(source.last_usage_date)
  display_name: Last Usage Date
  comment: Latest observed usage date, late records can still arrive.
- name: last_ingestion_date
  expr: MEASURE(source.last_ingestion_date)
  display_name: Last Ingestion Date
  comment: Latest ingestion date observed for the selected usage, not a completeness watermark.
materialization:
  schedule: every 6 hours
  mode: relaxed
  materialized_views:
  - name: daily_billing
    type: aggregated
    dimensions:
    - usage_date
    - usage_month
    - account_id
    - workspace_id
    - billing_origin_product
    measures:
    - priced_cost_usd
    - unpriced_record_count
    - record_count
    - dbu_usage
$$;