# Databricks notebook source
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "job_runs_article")
namespace = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"

spark.sql(
    "CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:namespace)",
    args={"namespace": namespace},
).collect()
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS IDENTIFIER(:table_name)
    (executed_at TIMESTAMP, message STRING)
    USING DELTA
    """,
    args={"table_name": f"{namespace}.deployment_events"},
).collect()

# COMMAND ----------
# Replace this small audit action with your own deployment notebook logic.
spark.sql(
    """
    INSERT INTO IDENTIFIER(:table_name)
    SELECT current_timestamp(), 'Deployment hook executed'
    """,
    args={"table_name": f"{namespace}.deployment_events"},
).collect()

display(spark.sql(
    "SELECT * FROM IDENTIFIER(:table_name) ORDER BY executed_at DESC",
    args={"table_name": f"{namespace}.deployment_events"},
))
print("Deployment hook completed.")
