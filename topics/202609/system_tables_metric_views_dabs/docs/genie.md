# Genie instructions and example questions

Create a Genie space/agent on the same warehouse after the deployment job succeeds. Add these curated **live** metric views:

```
main.system_tables_metrics.billing
main.system_tables_metrics.billing_monthly
main.system_tables_metrics.warehouse_queries
main.system_tables_metrics.warehouse_efficiency
```

Do not attach both live and materialized copies as independent facts: they represent the same consumption. A Genie resource is intentionally not deployed; this file supplies instructions/questions for an existing or newly configured space.

## Paste into the space's instructions

Our fiscal year begins on March 1 and ends on the last day of February. Fiscal year is named by its starting year: FY2026/27 starts on 2026-03-01. Q1 means March–May, Q2 June–August, Q3 September–November, and Q4 December–February. Always disambiguate the fiscal year when the user says only Q1 or Q4.

Unless explicitly requested otherwise, YTD means fiscal YTD, from March 1 through yesterday UTC, excluding today. For an exact YTD total use `billing` filtered by `usage_date`. Do not sum the monthly cumulative YTD snapshots.

“Spend”, “cost” and “consumption” mean `MEASURE(spend_usd)`: an estimated USD amount based on effective public list prices. Label results as estimates, not invoices or negotiated actual charges. Usage/DBUs are not dollars. Use `MEASURE(dbu_usage)` only for DBUs. Never sum unlike usage units.

Return `MEASURE(unpriced_record_count)` with cost answers. When it is greater than zero, `spend_usd` is unknown; explain the missing-price issue. `priced_cost_usd` is a subtotal, not a complete replacement. A missing period or NULL must not be silently replaced with zero. Retained source data and late arrivals can make comparisons incomplete.

Billing includes signed ORIGINAL, RETRACTION and RESTATEMENT records. Do not drop retractions or take absolute usage. The bundle already resolves price periods; do not rebuild a simplified price join.

“I” or “our” does not identify a workspace or apply a current-user cost filter. Ask which workspace/account is intended when ambiguous; otherwise state the scope explicitly. Preserve account and workspace identifiers when grouping warehouse IDs.

For prior-month or prior-year month comparisons use `billing_monthly` and its offset measures, grouping by `usage_month`. Compute the windowed result with its history available, then filter that result to the reporting months. Compare complete months unless partial-month comparison is explicitly requested. For same-elapsed fiscal-YTD comparison use matching daily date intervals, not all of the previous year.

Warehouse query telemetry is regional while billing is account-wide. Use `warehouse_efficiency` for blended cost-per-observed-statement questions and include `unmatched_warehouse_days`. This ratio is not exact query billing attribution and does not establish utilization. Never join raw billing events to raw query events. If combining metric views yourself, aggregate each with `MEASURE()` to the same keys before joining the resulting CTEs.

Use metric measures through `MEASURE()`. Do not use `SUM(spend_usd)` on a metric view or `SELECT *` expecting measure evaluation. DataFrame/CTE columns produced by an already-aggregated query are ordinary columns and may be used normally.

## Questions to try

1. How much have we spent on Databricks fiscal YTD, through yesterday UTC? Include missing-price records and tell me the workspace scope.
2. Break down estimated FY2026/27 Q1 spend by workspace and SKU. Show DBUs separately from USD.
3. Compare our last six complete months with the preceding month and the same month last year. Do not turn missing history into zero.
4. Which cost centers had the highest estimated spend this fiscal year, and how much consumption is untagged?
5. Which SQL warehouses had the highest blended list-price cost per observed statement over the last 14 complete days? Flag telemetry gaps.
6. How does this fiscal YTD compare with the same elapsed period of the prior fiscal year? Show observed date ranges so I can assess history coverage.
7. Which SKUs have nonzero usage with no matching full-interval USD price?

## Query patterns for saved examples

### Fiscal YTD, account-wide

```sql
SELECT
  MEASURE(spend_usd) AS estimated_spend_usd,
  MEASURE(unpriced_record_count) AS unpriced_records,
  MEASURE(last_usage_date) AS latest_observed_usage
FROM main.system_tables_metrics.billing
WHERE usage_date >= MAKE_DATE(YEAR(ADD_MONTHS(CURRENT_DATE(), -2)), 3, 1)
  AND usage_date < CURRENT_DATE();
```

Use a UTC SQL session for the examples in this file. Add `AND workspace_id = '<workspace ID>'` when the scope is known. The notebook demonstrates safely binding that value rather than interpolating it.

### Last six complete months with offsets

```sql
WITH history AS (
  SELECT usage_month,
         MEASURE(spend_usd) AS estimate_usd,
         MEASURE(previous_month_spend_usd) AS prior_month_usd,
         MEASURE(previous_year_spend_usd) AS same_month_last_year_usd,
         MEASURE(year_over_year_pct) AS yoy_pct
  FROM main.system_tables_metrics.billing_monthly
  GROUP BY usage_month
)
SELECT * FROM history
WHERE usage_month >= ADD_MONTHS(CAST(DATE_TRUNC('MONTH', CURRENT_DATE()) AS DATE), -6)
  AND usage_month < CAST(DATE_TRUNC('MONTH', CURRENT_DATE()) AS DATE)
ORDER BY usage_month;
```

### Fiscal quarter and SKU

```sql
SELECT workspace_id, sku_name,
       MEASURE(spend_usd) AS estimated_spend_usd,
       MEASURE(dbu_usage) AS dbus,
       MEASURE(unpriced_record_count) AS unpriced_records
FROM main.system_tables_metrics.billing
WHERE fiscal_year = 2026 AND fiscal_quarter = 'Q1'
GROUP BY workspace_id, sku_name
ORDER BY estimated_spend_usd DESC NULLS LAST;
```

### Metric-to-metric join result

```sql
SELECT account_id, workspace_id, warehouse_id, warehouse_name,
       MEASURE(spend_usd) AS estimated_spend_usd,
       MEASURE(query_count) AS statements,
       MEASURE(blended_usd_per_query) AS blended_usd_per_statement,
       MEASURE(unmatched_warehouse_days) AS unmatched_warehouse_days
FROM main.system_tables_metrics.warehouse_efficiency
WHERE usage_date >= DATE_SUB(CURRENT_DATE(), 14)
  AND usage_date < CURRENT_DATE()
GROUP BY account_id, workspace_id, warehouse_id, warehouse_name;
```

These are documented-syntax examples, not evidence of execution against your workspace. Relevant primary references: [4,7,8,10,13,14 in sources.md](sources.md).
