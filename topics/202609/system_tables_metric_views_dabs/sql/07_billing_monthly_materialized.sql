-- Databricks notebook source
-- Materialize monthly additive billing measures; time-window semantics are identical to the live monthly view.
-- SQL-only deployment notebook. The schema parameter comes from the native DAB schema resource.
SET TIME ZONE 'UTC';

-- COMMAND ----------

-- DBTITLE 1,Diagnostic test
-- Minimal parse test: does the serverless compute recognize $$ metric view syntax?
SELECT 'metric_view_ddl_supported' AS test
WHERE EXISTS (
  SELECT 1 FROM system.information_schema.routines LIMIT 0
);

-- COMMAND ----------

CREATE OR REPLACE VIEW IDENTIFIER('main.' || :schema || '.billing_monthly')
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: Monthly billing and date offsets. Source measures come from canonical billing. March fiscal year. Window
  values are snapshots, not additive across months. Current month can be partial. Managed materialization of additive
  monthly measures offset YTD queries may use source fallback.
source: |
  SELECT account_id, workspace_id, usage_month AS month_start,
         MEASURE(priced_cost_usd) AS monthly_priced_cost_usd,
         MEASURE(unpriced_record_count) AS monthly_unpriced_records,
         MEASURE(record_count) AS monthly_records,
         MEASURE(dbu_usage) AS monthly_dbus
  FROM main.system_tables_metrics.billing
  GROUP BY account_id, workspace_id, usage_month
dimensions:
- name: usage_month
  expr: source.month_start
  display_name: Usage Month
  comment: Monthly time-series anchor keep all history inside offset calculations.
- name: fiscal_year_start
  expr: CAST(ADD_MONTHS(DATE_TRUNC('YEAR', ADD_MONTHS(usage_month, -2)), 2) AS DATE)
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
  expr: MONTH(ADD_MONTHS(usage_month, -2))
  display_name: Fiscal Month Number
  comment: March=1, February=12.
- name: fiscal_quarter
  expr: CONCAT('Q', QUARTER(ADD_MONTHS(usage_month, -2)))
  display_name: Fiscal Quarter
  comment: Q1 Mar-May Q2 Jun-Aug Q3 Sep-Nov Q4 Dec-Feb. Always pair with fiscal_year.
- name: fiscal_quarter_start
  expr: CAST(ADD_MONTHS(DATE_TRUNC('QUARTER', ADD_MONTHS(usage_month, -2)), 2) AS DATE)
  display_name: Fiscal Quarter Start
- name: account_id
  expr: source.account_id
  display_name: Account Id
- name: workspace_id
  expr: source.workspace_id
  display_name: Workspace Id
measures:
- name: priced_cost_usd
  expr: SUM(source.monthly_priced_cost_usd)
  display_name: Priced Cost Usd
  comment: Priced subtotal, additive across months. Reuses canonical billing.
- name: unpriced_record_count
  expr: SUM(source.monthly_unpriced_records)
  display_name: Unpriced Record Count
  comment: Unpriced physical records, additive across months.
- name: record_count
  expr: SUM(source.monthly_records)
  display_name: Record Count
  comment: Physical billing records, additive across months.
- name: dbu_usage
  expr: SUM(source.monthly_dbus)
  display_name: Dbu Usage
  comment: Signed DBUs, additive across months.
- name: spend_usd
  expr: CASE WHEN MEASURE(unpriced_record_count) = 0 THEN MEASURE(priced_cost_usd) END
  display_name: Spend Usd
  comment: Guarded public-list-price USD estimate additive total, not a window snapshot.
- name: previous_month_priced_cost_usd
  expr: SUM(source.monthly_priced_cost_usd)
  display_name: Previous Month Priced Cost Usd
  comment: Subtotal at offset -1 month. NULL outside available history.
  window:
  - order: usage_month
    range: current
    semiadditive: last
    offset: -1 month
- name: previous_month_unpriced_records
  expr: SUM(source.monthly_unpriced_records)
  display_name: Previous Month Unpriced Records
  comment: Quality count at the same -1 month offset.
  window:
  - order: usage_month
    range: current
    semiadditive: last
    offset: -1 month
- name: previous_month_spend_usd
  expr: CASE WHEN MEASURE(previous_month_unpriced_records) = 0 THEN MEASURE(previous_month_priced_cost_usd) END
  display_name: Previous Month Spend Usd
  comment: Guarded previous month estimate missing history is NULL, not zero.
- name: previous_year_priced_cost_usd
  expr: SUM(source.monthly_priced_cost_usd)
  display_name: Previous Year Priced Cost Usd
  comment: Subtotal at offset -12 month. NULL outside available history.
  window:
  - order: usage_month
    range: current
    semiadditive: last
    offset: -12 month
- name: previous_year_unpriced_records
  expr: SUM(source.monthly_unpriced_records)
  display_name: Previous Year Unpriced Records
  comment: Quality count at the same -12 month offset.
  window:
  - order: usage_month
    range: current
    semiadditive: last
    offset: -12 month
- name: previous_year_spend_usd
  expr: CASE WHEN MEASURE(previous_year_unpriced_records) = 0 THEN MEASURE(previous_year_priced_cost_usd) END
  display_name: Previous Year Spend Usd
  comment: Guarded previous year estimate missing history is NULL, not zero.
- name: fiscal_ytd_priced_cost_usd
  expr: SUM(source.monthly_priced_cost_usd)
  display_name: Fiscal Ytd Priced Cost Usd
  comment: Cumulative subtotal since March 1 as of the selected month includes any observed current-month data.
  window:
  - order: usage_month
    range: cumulative
    semiadditive: last
  - order: fiscal_year_start
    range: current
    semiadditive: last
- name: fiscal_ytd_unpriced_records
  expr: SUM(source.monthly_unpriced_records)
  display_name: Fiscal Ytd Unpriced Records
  comment: Cumulative price-quality count since March 1.
  window:
  - order: usage_month
    range: cumulative
    semiadditive: last
  - order: fiscal_year_start
    range: current
    semiadditive: last
- name: fiscal_ytd_spend_usd
  expr: CASE WHEN MEASURE(fiscal_ytd_unpriced_records) = 0 THEN MEASURE(fiscal_ytd_priced_cost_usd) END
  display_name: Fiscal Ytd Spend Usd
  comment: March-year cumulative snapshot at each month. Do not SUM these monthly YTD snapshots.
- name: month_over_month_pct
  expr: 100.0 * TRY_DIVIDE(MEASURE(spend_usd) - MEASURE(previous_month_spend_usd), MEASURE(previous_month_spend_usd))
  display_name: Month Over Month Pct
  comment: Percent change, e.g. 10 means +10%. Query by usage_month, not across multiple months.
- name: year_over_year_pct
  expr: 100.0 * TRY_DIVIDE(MEASURE(spend_usd) - MEASURE(previous_year_spend_usd), MEASURE(previous_year_spend_usd))
  display_name: Year Over Year Pct
  comment: Same month prior year 10 means +10%. Query by usage_month. Missing/zero base produces NULL.
materialization:
  schedule: every 6 hours
  mode: relaxed
  materialized_views:
  - name: monthly_billing
    type: aggregated
    dimensions:
    - usage_month
    - account_id
    - workspace_id
    measures:
    - priced_cost_usd
    - unpriced_record_count
    - record_count
    - dbu_usage
$$;

-- COMMAND ----------

-- DBTITLE 1,Parse test
-- Minimal metric view DDL parse test
CREATE OR REPLACE VIEW main.system_tables_metrics_materialized.__parse_test
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: system.billing.usage
dimensions:
- name: usage_date
  expr: source.usage_date
measures:
- name: cnt
  expr: COUNT(1)
$$;