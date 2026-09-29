-- Databricks notebook source
-- Validate deployed metric syntax and report pricing quality. Refresh completion is checked separately, not assumed.
SET TIME ZONE 'UTC';

-- COMMAND ----------

SELECT fiscal_year, fiscal_quarter, MEASURE(spend_usd) AS estimated_spend_usd,
       MEASURE(unpriced_record_count) AS unpriced_records, MEASURE(dbu_usage) AS dbus
FROM main.system_tables_metrics.billing
WHERE usage_date >= DATE_SUB(CURRENT_DATE(), 7) AND usage_date < CURRENT_DATE()
GROUP BY fiscal_year, fiscal_quarter;

-- COMMAND ----------

-- Keep the offset query's history. Apply the reporting range to its result, not its input.
WITH periods AS (
  SELECT usage_month, MEASURE(spend_usd) AS estimated_spend_usd,
         MEASURE(previous_month_spend_usd) AS previous_month_spend_usd,
         MEASURE(previous_year_spend_usd) AS previous_year_spend_usd,
         MEASURE(fiscal_ytd_spend_usd) AS fiscal_ytd_spend_usd
  FROM main.system_tables_metrics.billing_monthly
  GROUP BY usage_month
)
SELECT * FROM periods
WHERE usage_month >= ADD_MONTHS(CAST(DATE_TRUNC('MONTH', CURRENT_DATE()) AS DATE), -3)
  AND usage_month < CAST(DATE_TRUNC('MONTH', CURRENT_DATE()) AS DATE)
ORDER BY usage_month;

-- COMMAND ----------

SELECT workspace_id, warehouse_id, MEASURE(query_count) AS statements,
       MEASURE(failure_rate_pct) AS failure_pct
FROM main.system_tables_metrics.warehouse_queries
WHERE query_date >= DATE_SUB(CURRENT_DATE(), 7) AND query_date < CURRENT_DATE()
GROUP BY workspace_id, warehouse_id LIMIT 20;

-- COMMAND ----------

SELECT warehouse_id, MEASURE(spend_usd) AS estimated_spend_usd,
       MEASURE(query_count) AS statements,
       MEASURE(blended_usd_per_query) AS blended_usd_per_query,
       MEASURE(unmatched_warehouse_days) AS unmatched_warehouse_days
FROM main.system_tables_metrics.warehouse_efficiency
WHERE usage_date >= DATE_SUB(CURRENT_DATE(), 7) AND usage_date < CURRENT_DATE()
GROUP BY warehouse_id LIMIT 20;

-- COMMAND ----------

SELECT usage_month, MEASURE(priced_cost_usd) AS priced_subtotal_usd,
       MEASURE(unpriced_record_count) AS unpriced_records
FROM main.system_tables_metrics_materialized.billing
WHERE usage_date >= DATE_SUB(CURRENT_DATE(), 7) AND usage_date < CURRENT_DATE()
GROUP BY usage_month;

-- COMMAND ----------

SELECT usage_month, MEASURE(priced_cost_usd) AS priced_subtotal_usd,
       MEASURE(unpriced_record_count) AS unpriced_records
FROM main.system_tables_metrics_materialized.billing_monthly
WHERE usage_month >= ADD_MONTHS(CAST(DATE_TRUNC('MONTH', CURRENT_DATE()) AS DATE), -3)
GROUP BY usage_month;

-- COMMAND ----------

-- Successful queries can be source fallbacks. Inspect the pipeline/refresh information here,
-- then run notebooks/20_materialization.py to inspect EXPLAIN after the initial refresh.
DESCRIBE TABLE EXTENDED main.system_tables_metrics_materialized.billing;

-- COMMAND ----------

DESCRIBE TABLE EXTENDED main.system_tables_metrics_materialized.billing_monthly;
