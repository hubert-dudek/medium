-- Databricks notebook source
-- Demonstrate fixed/coarser/finer grains with documented SQL, without inventing a native metric-view LOD syntax.
SET TIME ZONE 'UTC';

-- COMMAND ----------

-- FIXED GRAIN: retained-history cost by workspace. This is not full customer lifetime beyond source retention.
-- Filters in the outer query do not re-evaluate the already-aggregated workspace result.
WITH workspace_cost AS (
  SELECT account_id, workspace_id, MEASURE(spend_usd) AS retained_history_spend_usd,
         MEASURE(unpriced_record_count) AS unpriced_records
  FROM main.system_tables_metrics.billing
  GROUP BY account_id, workspace_id
)
SELECT * FROM workspace_cost
WHERE retained_history_spend_usd > 1000
ORDER BY retained_history_spend_usd DESC;

-- COMMAND ----------

-- COARSER GRAIN: each workspace's share of its ACCOUNT total this fiscal year.
-- If any workspace is unpriced, suppress all percentages for that account instead of using a partial denominator.
WITH by_workspace AS (
  SELECT account_id, workspace_id, MEASURE(spend_usd) AS spend_usd,
         MEASURE(unpriced_record_count) AS missing
  FROM main.system_tables_metrics.billing
  WHERE usage_date >= MAKE_DATE(YEAR(ADD_MONTHS(CURRENT_DATE(), -2)), 3, 1)
    AND usage_date < CURRENT_DATE()
  GROUP BY account_id, workspace_id
)
SELECT account_id, workspace_id, spend_usd,
       CASE WHEN SUM(missing) OVER (PARTITION BY account_id) = 0
            THEN 100.0 * TRY_DIVIDE(spend_usd, SUM(spend_usd) OVER (PARTITION BY account_id)) END AS account_share_pct
FROM by_workspace
ORDER BY account_id, account_share_pct DESC;

-- COMMAND ----------

-- FINER GRAIN: calculate each workspace-day first, then find the maximum daily cost per workspace.
-- Maximum is withheld when any observed day in that workspace was unpriced.
WITH daily AS (
  SELECT account_id, workspace_id, usage_date, MEASURE(spend_usd) AS day_spend_usd,
         MEASURE(unpriced_record_count) AS missing
  FROM main.system_tables_metrics.billing
  WHERE usage_date >= DATE_SUB(CURRENT_DATE(), 30) AND usage_date < CURRENT_DATE()
  GROUP BY account_id, workspace_id, usage_date
)
SELECT account_id, workspace_id,
       CASE WHEN SUM(missing) = 0 THEN MAX(day_spend_usd) END AS maximum_daily_spend_usd,
       SUM(missing) AS unpriced_records
FROM daily GROUP BY account_id, workspace_id
ORDER BY maximum_daily_spend_usd DESC NULLS LAST;
