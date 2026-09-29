-- Databricks notebook source
-- Define regional SQL warehouse query metrics without materializing a masked query-history source.
-- SQL-only deployment notebook. The schema parameter comes from the native DAB schema resource.
SET TIME ZONE 'UTC';

-- COMMAND ----------

CREATE OR REPLACE VIEW IDENTIFIER('main.' || :schema || '.warehouse_queries')
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: SQL warehouse query metrics for the current region only. Excludes statement text and identities. Terminal
  statements are counted by UTC start date.
source: |
  SELECT account_id, workspace_id, compute.warehouse_id AS warehouse_id,
         statement_id, CAST(start_time AS DATE) AS query_date, execution_status,
         total_duration_ms, execution_duration_ms
  FROM system.query.history
  WHERE compute.type = 'WAREHOUSE'
    AND compute.warehouse_id IS NOT NULL
    AND execution_status IN ('FINISHED', 'FAILED', 'CANCELED')
joins:
- name: warehouses
  source: main.system_tables_metrics.warehouse_latest
  'on': source.account_id = warehouses.account_id AND source.workspace_id = warehouses.workspace_id AND source.warehouse_id
    = warehouses.warehouse_id
dimensions:
- name: query_date
  expr: source.query_date
  display_name: Query Date
  comment: UTC query start date, cross-midnight duration is attributed to its start date.
- name: query_month
  expr: CAST(DATE_TRUNC('MONTH', query_date) AS DATE)
  display_name: Query Month
- name: fiscal_year
  expr: YEAR(ADD_MONTHS(query_date, -2))
  display_name: Fiscal Year
  comment: Fiscal start year, March-February.
- name: fiscal_quarter
  expr: CONCAT('Q', QUARTER(ADD_MONTHS(query_date, -2)))
  display_name: Fiscal Quarter
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
- name: execution_status
  expr: source.execution_status
  display_name: Execution Status
measures:
- name: query_count
  expr: COUNT(1)
  display_name: Query Count
  comment: All observed terminal warehouse statements in this region, including failures and cancellations.
- name: failed_query_count
  expr: COUNT(1) FILTER (WHERE source.execution_status = 'FAILED')
  display_name: Failed Query Count
  comment: Failed warehouse statements.
- name: execution_seconds
  expr: SUM(source.execution_duration_ms) / 1000.0
  display_name: Execution Seconds
  comment: Sum of wall-clock execution durations, overlapping statements can exceed elapsed wall time.
- name: total_duration_seconds
  expr: SUM(source.total_duration_ms) / 1000.0
  display_name: Total Duration Seconds
  comment: Sum of statement total duration, excluding result fetch.
- name: average_duration_seconds
  expr: TRY_DIVIDE(MEASURE(total_duration_seconds), MEASURE(query_count))
  display_name: Average Duration Seconds
  comment: Blended duration per observed statement, not compute utilization.
- name: failure_rate_pct
  expr: 100.0 * TRY_DIVIDE(MEASURE(failed_query_count), MEASURE(query_count))
  display_name: Failure Rate Pct
  comment: Percent failed, 10 means 10%.
$$;