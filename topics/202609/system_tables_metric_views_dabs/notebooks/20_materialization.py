# Databricks notebook source
# Inspect real metric-view materialization and optionally request a charged refresh. Read-only by default.

# COMMAND ----------

# MAGIC %md
# MAGIC # Verify the managed materialization
# MAGIC Run after deployment on compatible serverless notebook compute or Runtime 18.1+.
# MAGIC The initial refresh is asynchronous. A successful query does **not** demonstrate acceleration:
# MAGIC the optimizer can fall back to the source. No separate DAB pipeline is required.
# MAGIC `refresh_now=true` starts refresh work and can incur charges. Keep it false to inspect only.

# COMMAND ----------

dbutils.widgets.dropdown("refresh_now", "false", ["false", "true"])
spark.sql("SET TIME ZONE 'UTC'")
views = ["main.system_tables_metrics_materialized.billing",
         "main.system_tables_metrics_materialized.billing_monthly"]
for view in views:
    print(view)
    display(spark.sql("DESCRIBE TABLE EXTENDED IDENTIFIER(:view)", args={"view": view}))

# COMMAND ----------

if dbutils.widgets.get("refresh_now") == "true":
    # The source schemas and materialized-view owner must satisfy all privileges and serverless prerequisites.
    # This is manual operational work, not something to run repeatedly in a tight polling loop.
    for view in views:
        spark.sql("REFRESH MATERIALIZED VIEW IDENTIFIER(:view)", args={"view": view}).collect()
        print("Refresh statement completed:", view)
else:
    print("No refresh requested. Managed refresh schedules are every 6 hours.")

# COMMAND ----------

query = """
SELECT usage_month, MEASURE(priced_cost_usd) AS priced_subtotal_usd,
       MEASURE(unpriced_record_count) AS unpriced_records
FROM main.system_tables_metrics_materialized.billing
WHERE usage_date >= DATE_SUB(CURRENT_DATE(), 90) AND usage_date < CURRENT_DATE()
GROUP BY usage_month
"""
plan = "\n".join(str(row[0]) for row in spark.sql("EXPLAIN EXTENDED " + query).collect())
print(plan)
if "__materialization_mat_" in plan or "__metric_view_mat_" in plan:
    print("An internal materialization marker appears in the plan. Confirm the scan in the query profile.")
else:
    print("No documented materialization scan marker found. Check refresh status and query profile; this may be source fallback.")
display(spark.sql(query))

# COMMAND ----------

# Compare results for inspection, NOT as an equality assertion: relaxed materialization can be stale.
for schema in ["system_tables_metrics", "system_tables_metrics_materialized"]:
    print(schema)
    display(spark.sql("""
    SELECT usage_month, MEASURE(spend_usd) AS estimated_spend_usd,
           MEASURE(unpriced_record_count) AS unpriced_records
    FROM IDENTIFIER(:view)
    WHERE usage_date >= DATE_SUB(CURRENT_DATE(), 90) AND usage_date < CURRENT_DATE()
    GROUP BY usage_month ORDER BY usage_month
    """, args={"view": f"main.{schema}.billing"}))
# Incremental execution is an engine decision, not a promise. Inspect the managed pipeline's update details.
