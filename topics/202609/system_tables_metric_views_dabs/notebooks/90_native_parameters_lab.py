# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# Opt-in native metric-view parameter lab. Requires Runtime/warehouse engine 18.2+, independent of core deployment.

# COMMAND ----------

# MAGIC %md
# MAGIC # Native parameters, separately
# MAGIC Current public documentation supports native parameters on 18.2+; they are not just notebook widgets.
# MAGIC YAML parameter names are referenced **without a colon**. Query arguments use `=>`.
# MAGIC Notebook `:name` bind parameters safely supply the argument values.
# MAGIC This lab creates only a **session-local temporary metric view**. It never materializes a parameterized view.
# MAGIC The discount is a hypothetical scenario, not a claim about your contract.

# COMMAND ----------

dbutils.widgets.dropdown("run_parameter_lab", "false", ["false", "true"])
dbutils.widgets.text("discount_rate", "0.15")
dbutils.widgets.text("comparison_months", "-12")

import math
try:
    discount = float(dbutils.widgets.get("discount_rate"))
    comparison = int(dbutils.widgets.get("comparison_months"))
except ValueError as exc:
    raise ValueError("discount_rate must be a number; comparison_months must be an integer") from exc
if not math.isfinite(discount) or not 0 <= discount <= 1:
    raise ValueError("discount_rate must be between 0 and 1")
if not -120 <= comparison <= -1:
    raise ValueError("comparison_months must be -120 through -1")
spark.sql("SET TIME ZONE 'UTC'")

# COMMAND ----------

# Native parameters are part of the view definition, not string-replaced SQL constants.
spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW billing_scenario_lab
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
source: |
  SELECT usage_month AS month_start,
         MEASURE(priced_cost_usd) AS amount,
         MEASURE(unpriced_record_count) AS missing
  FROM main.system_tables_metrics.billing_monthly
  GROUP BY usage_month
parameters:
  - name: discount_rate
    data_type: double
    default: 0
  - name: comparison_months
    data_type: int
    default: -12
dimensions:
  - name: usage_month
    expr: source.month_start
measures:
  - name: unpriced_records
    expr: SUM(source.missing)
  - name: list_spend_usd
    expr: CASE WHEN MEASURE(unpriced_records) = 0 THEN SUM(source.amount) END
  - name: scenario_priced_usd
    expr: SUM((1 - discount_rate) * source.amount)
  - name: scenario_spend_usd
    expr: CASE WHEN MEASURE(unpriced_records) = 0 THEN MEASURE(scenario_priced_usd) END
  - name: prior_scenario_priced_usd
    expr: SUM((1 - discount_rate) * source.amount)
    window:
      - order: usage_month
        range: current
        semiadditive: last
        offset: comparison_months month
  - name: prior_unpriced_records
    expr: SUM(source.missing)
    window:
      - order: usage_month
        range: current
        semiadditive: last
        offset: comparison_months month
  - name: prior_scenario_spend_usd
    expr: CASE WHEN MEASURE(prior_unpriced_records) = 0 THEN MEASURE(prior_scenario_priced_usd) END
$$
""").collect()

# COMMAND ----------

display(spark.sql("""
WITH history AS (
  SELECT usage_month, MEASURE(list_spend_usd) AS list_estimate_usd,
         MEASURE(scenario_spend_usd) AS scenario_estimate_usd,
         MEASURE(prior_scenario_spend_usd) AS prior_scenario_estimate_usd
  FROM billing_scenario_lab(discount_rate => :discount, comparison_months => :comparison)
  GROUP BY usage_month
)
SELECT * FROM history
WHERE usage_month >= ADD_MONTHS(CAST(DATE_TRUNC('MONTH', CURRENT_DATE()) AS DATE), -6)
  AND usage_month < CAST(DATE_TRUNC('MONTH', CURRENT_DATE()) AS DATE)
ORDER BY usage_month
""", args={"discount": discount, "comparison": comparison}))

# COMMAND ----------

# Default parameters: no discount and a -12 month comparison.
display(spark.sql("""
SELECT usage_month, MEASURE(scenario_spend_usd) AS undiscounted_estimate_usd
FROM billing_scenario_lab()
GROUP BY usage_month ORDER BY usage_month DESC LIMIT 6
"""))
# No broad exception handler: unsupported feature or permissions errors should remain visible.
# The temporary view disappears with this session; it is not a third UC schema or a deployed dependency.