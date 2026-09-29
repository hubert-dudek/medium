-- Databricks notebook source
-- Join billing and query metric results at a safe daily grain, avoiding a raw fact-to-fact fanout.
-- SQL-only deployment notebook. The schema parameter comes from the native DAB schema resource.
SET TIME ZONE 'UTC';

-- COMMAND ----------

CREATE OR REPLACE VIEW IDENTIFIER('main.' || :schema || '.warehouse_efficiency')
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: Joins two independently aggregated metric views at account/workspace/warehouse/day grain. Public-list-price
  cost per observed statement is blended, NOT actual per-query billing attribution. Regional query coverage differs
  from billing.
source: |
  WITH cost AS (
    SELECT account_id, workspace_id, warehouse_id, usage_date AS day,
           MEASURE(priced_cost_usd) AS priced_cost,
           MEASURE(unpriced_record_count) AS unpriced_records,
           MEASURE(record_count) AS billing_records
    FROM main.system_tables_metrics.billing
    WHERE warehouse_id IS NOT NULL
    GROUP BY account_id, workspace_id, warehouse_id, usage_date
  ), queries AS (
    SELECT account_id, workspace_id, warehouse_id, query_date AS day,
           MEASURE(query_count) AS statements,
           MEASURE(execution_seconds) AS execution_seconds
    FROM main.system_tables_metrics.warehouse_queries
    GROUP BY account_id, workspace_id, warehouse_id, query_date
  )
  SELECT COALESCE(c.account_id, q.account_id) AS account_id,
         COALESCE(c.workspace_id, q.workspace_id) AS workspace_id,
         COALESCE(c.warehouse_id, q.warehouse_id) AS warehouse_id,
         COALESCE(c.day, q.day) AS day,
         c.priced_cost, c.unpriced_records, c.billing_records,
         q.statements, q.execution_seconds,
         CASE WHEN c.billing_records IS NOT NULL AND q.statements IS NOT NULL
              THEN 'both_sources'
              WHEN c.billing_records IS NOT NULL THEN 'billing_only'
              ELSE 'queries_only' END AS coverage
  FROM cost c
  FULL OUTER JOIN queries q
    ON c.account_id = q.account_id AND c.workspace_id = q.workspace_id
   AND c.warehouse_id = q.warehouse_id AND c.day = q.day
joins:
- name: warehouses
  source: main.system_tables_metrics.warehouse_latest
  'on': source.account_id = warehouses.account_id AND source.workspace_id = warehouses.workspace_id AND source.warehouse_id
    = warehouses.warehouse_id
dimensions:
- name: usage_date
  expr: source.day
  display_name: Usage Date
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
  comment: Q1 Mar-May, Q2 Jun-Aug, Q3 Sep-Nov, Q4 Dec-Feb. Always pair with fiscal_year.
- name: fiscal_quarter_start
  expr: CAST(ADD_MONTHS(DATE_TRUNC('QUARTER', ADD_MONTHS(usage_date, -2)), 2) AS DATE)
  display_name: Fiscal Quarter Start
- name: account_id
  expr: source.account_id
  display_name: Account Id
- name: workspace_id
  expr: source.workspace_id
  display_name: Workspace Id
- name: warehouse_id
  expr: source.warehouse_id
  display_name: Warehouse Id
- name: warehouse_name
  expr: warehouses.warehouse_name
  display_name: Warehouse Name
- name: coverage
  expr: source.coverage
  display_name: Coverage
  comment: both_sources, billing_only or queries_only. Absence does not mean zero.
measures:
- name: priced_cost_usd
  expr: SUM(source.priced_cost)
  display_name: Priced Cost Usd
  comment: Matched-price warehouse billing subtotal, includes idle cost.
- name: unpriced_record_count
  expr: SUM(source.unpriced_records)
  display_name: Unpriced Record Count
  comment: Unpriced records on warehouse billing days.
- name: spend_usd
  expr: CASE WHEN MEASURE(unpriced_record_count) = 0 THEN MEASURE(priced_cost_usd) END
  display_name: Spend Usd
  comment: Guarded warehouse USD public-list-price estimate.
- name: query_count
  expr: SUM(source.statements)
  display_name: Query Count
  comment: Observed terminal warehouse statements in this region.
- name: execution_seconds
  expr: SUM(source.execution_seconds)
  display_name: Execution Seconds
  comment: Observed execution durations, not DBU attribution.
- name: unmatched_warehouse_days
  expr: SUM(CASE WHEN source.coverage <> 'both_sources' THEN 1 ELSE 0 END)
  display_name: Unmatched Warehouse Days
  comment: Warehouse-days missing either source. A conservative coverage warning.
- name: blended_usd_per_query
  expr: CASE WHEN MEASURE(unmatched_warehouse_days) = 0 THEN TRY_DIVIDE(MEASURE(spend_usd), MEASURE(query_count))
    END
  display_name: Blended Usd Per Query
  comment: Blended list cost / observed statements, NULL for unmatched days or incomplete pricing. Not statement-level
    invoiced cost.
$$;