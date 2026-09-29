# Databricks notebook source
# Query the deployed semantic layer from PySpark using named parameters and explicit UC identifiers.
# Attach to compatible serverless notebook compute or a UC-enabled cluster with Runtime 18.1+.

# COMMAND ----------

# MAGIC %md
# MAGIC # Engineering examples
# MAGIC These are read-only examples. `spend_usd` means **estimated USD at effective public list prices**,
# MAGIC not the amount on your invoice. An empty workspace filter means all accessible billing workspaces.
# MAGIC Business years run **March 1 through the last day of February**. Dates below are UTC.
# MAGIC Offsets use all available history; missing historical months remain NULL.

# COMMAND ----------

from datetime import date, datetime, timezone

# Every persistent identifier is catalog.schema.object. The schema input is intentionally allowlisted.
dbutils.widgets.text("schema", "system_tables_metrics")
dbutils.widgets.text("workspace_id", "")
dbutils.widgets.text("cutoff_date_exclusive", "")

schema = dbutils.widgets.get("schema").strip()
if schema not in {"system_tables_metrics", "system_tables_metrics_materialized"}:
    raise ValueError("schema must be system_tables_metrics or system_tables_metrics_materialized")
workspace_id = dbutils.widgets.get("workspace_id").strip()
cutoff_text = dbutils.widgets.get("cutoff_date_exclusive").strip()
try:
    cutoff = date.fromisoformat(cutoff_text) if cutoff_text else datetime.now(timezone.utc).date()
except ValueError as exc:
    raise ValueError("cutoff_date_exclusive must be blank or YYYY-MM-DD") from exc
fy_start = date(cutoff.year if cutoff.month >= 3 else cutoff.year - 1, 3, 1)

spark.sql("SET TIME ZONE 'UTC'")
args = {
    "billing_view": f"main.{schema}.billing",
    "monthly_view": f"main.{schema}.billing_monthly",
    "workspace_id": workspace_id,
    "fy_start": fy_start,
    "cutoff": cutoff,
}
print(f"FY{fy_start.year}/{str(fy_start.year + 1)[-2:]}: [{fy_start}, {cutoff}) UTC")
print("Scope:", workspace_id or "all accessible billing workspaces")
print("Materialized results may lag; a latest usage date is not a completeness guarantee.")

# COMMAND ----------

# 1. How much have I spent fiscal YTD? Uses precise daily boundaries, not a sum of YTD snapshots.
fytd_df = spark.sql("""
SELECT MEASURE(spend_usd) AS estimated_spend_usd,
       MEASURE(priced_cost_usd) AS priced_subtotal_usd,
       MEASURE(unpriced_record_count) AS unpriced_records,
       MEASURE(record_count) AS billing_records,
       MEASURE(dbu_usage) AS dbus,
       MEASURE(first_usage_date) AS first_observed_usage,
       MEASURE(last_usage_date) AS last_observed_usage,
       MEASURE(last_ingestion_date) AS last_observed_ingestion
FROM IDENTIFIER(:billing_view)
WHERE usage_date >= :fy_start AND usage_date < :cutoff
  AND (:workspace_id = '' OR workspace_id = :workspace_id)
""", args=args)
display(fytd_df)
# No rows/prior history is not proof of zero spend. On March 1 the current FYTD date interval is empty.

# COMMAND ----------

# 2. Quarter and SKU drill-down. Fiscal-year grouping prevents Q1 from different years being combined.
display(spark.sql("""
SELECT fiscal_year_label, fiscal_quarter, sku_name,
       MEASURE(spend_usd) AS estimated_spend_usd,
       MEASURE(unpriced_record_count) AS unpriced_records
FROM IDENTIFIER(:billing_view)
WHERE usage_date >= :fy_start AND usage_date < :cutoff
  AND (:workspace_id = '' OR workspace_id = :workspace_id)
GROUP BY fiscal_year_label, fiscal_quarter, sku_name
ORDER BY estimated_spend_usd DESC NULLS LAST
LIMIT 50
""", args=args))

# COMMAND ----------

# 3. Native -1 month / -12 month offsets, plus March fiscal-YTD snapshots.
# Time filtering is OUTSIDE the windowed query. Do not put fiscal_year = 2026 inside it.
# Six complete months only; the in-progress month is intentionally not compared to a full prior month.
monthly_df = spark.sql("""
WITH history AS (
  SELECT usage_month,
         MEASURE(spend_usd) AS estimated_spend_usd,
         MEASURE(previous_month_spend_usd) AS previous_month_spend_usd,
         MEASURE(previous_year_spend_usd) AS same_month_previous_year_usd,
         MEASURE(month_over_month_pct) AS mom_pct,
         MEASURE(year_over_year_pct) AS yoy_pct,
         MEASURE(fiscal_ytd_spend_usd) AS fiscal_ytd_snapshot_usd
  FROM IDENTIFIER(:monthly_view)
  WHERE (:workspace_id = '' OR workspace_id = :workspace_id)
  GROUP BY usage_month
)
SELECT * FROM history
WHERE usage_month >= ADD_MONTHS(CAST(DATE_TRUNC('MONTH', :cutoff) AS DATE), -6)
  AND usage_month < CAST(DATE_TRUNC('MONTH', :cutoff) AS DATE)
ORDER BY usage_month
""", args=args)
display(monthly_df)
# Do not SUM fiscal_ytd_snapshot_usd, or the offset snapshots, over multiple months.

# COMMAND ----------

# 4. Compare the same elapsed fiscal period with last year. This is not FYTD vs all of last FY.
# ADD_MONTHS clamps leap-day boundaries to the last valid day of the target month.
display(spark.sql("""
SELECT CASE WHEN usage_date >= :fy_start THEN 'current_fytd' ELSE 'previous_fytd' END AS period,
       MEASURE(spend_usd) AS estimated_spend_usd,
       MEASURE(unpriced_record_count) AS unpriced_records,
       MEASURE(first_usage_date) AS first_observed_usage,
       MEASURE(last_usage_date) AS last_observed_usage
FROM IDENTIFIER(:billing_view)
WHERE (:workspace_id = '' OR workspace_id = :workspace_id)
  AND ((usage_date >= :fy_start AND usage_date < :cutoff)
       OR (usage_date >= ADD_MONTHS(:fy_start, -12) AND usage_date < ADD_MONTHS(:cutoff, -12)))
GROUP BY CASE WHEN usage_date >= :fy_start THEN 'current_fytd' ELSE 'previous_fytd' END
ORDER BY period
""", args=args))
# Earlier fiscal history may already be outside system-table retention. Inspect coverage before comparing.

# COMMAND ----------

# 5. Consume the metric view built from JOINED metric-view results.
# Its implementation in sql/05_warehouse_efficiency.sql aggregates both sources BEFORE the join.
# These views are intentionally only in the live schema: query-history masking can prevent materialization.
display(spark.sql("""
SELECT workspace_id, warehouse_id, warehouse_name,
       MEASURE(spend_usd) AS estimated_spend_usd,
       MEASURE(query_count) AS statements,
       MEASURE(blended_usd_per_query) AS blended_list_usd_per_statement,
       MEASURE(unmatched_warehouse_days) AS unmatched_warehouse_days
FROM main.system_tables_metrics.warehouse_efficiency
WHERE usage_date >= DATE_SUB(:cutoff, 14) AND usage_date < :cutoff
  AND (:workspace_id = '' OR workspace_id = :workspace_id)
GROUP BY workspace_id, warehouse_id, warehouse_name
ORDER BY estimated_spend_usd DESC NULLS LAST
LIMIT 30
""", args=args))
# Blended cost per statement is NOT an allocation of an individual statement's billed DBUs.

# COMMAND ----------

# 6. Inspect missing prices instead of treating missing cost as zero.
display(spark.sql("""
SELECT sku_name, cloud, usage_unit, price_status,
       MEASURE(record_count) AS records,
       MEASURE(usage_quantity_single_unit) AS quantity
FROM IDENTIFIER(:billing_view)
WHERE usage_date >= :fy_start AND usage_date < :cutoff
  AND (:workspace_id = '' OR workspace_id = :workspace_id)
  AND price_status = 'unpriced'
GROUP BY sku_name, cloud, usage_unit, price_status
ORDER BY records DESC
""", args=args))

# COMMAND ----------

# 7. Once MEASURE() has produced a DataFrame, normal engineering operations work as usual.
from pyspark.sql import functions as F

engineering_result = (fytd_df
    .withColumn("fiscal_year_start", F.lit(fy_start))
    .withColumn("cutoff_date_exclusive", F.lit(cutoff))
    .withColumn("pricing_basis", F.lit("USD effective public list price")))
engineering_result.createOrReplaceTempView("billing_fytd_engineering_result")
display(spark.sql("SELECT * FROM billing_fytd_engineering_result"))
# Optional downstream write, deliberately not executed by Run All:
# engineering_result.write.format("delta").mode("append").saveAsTable("main.your_schema.billing_snapshots")
