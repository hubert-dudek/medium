# Databricks notebook source
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "news_202620_dabs")
namespace = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"

spark.sql(
    "CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:namespace)",
    args={"namespace": namespace},
).collect()

# COMMAND ----------
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS IDENTIFIER(:table_name)
    USING DELTA
    AS SELECT 'timezone' AS setting_name, 'UTC' AS setting_value
    """,
    args={"table_name": f"{namespace}.bootstrap_settings"},
).collect()

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS IDENTIFIER(:table_name)
    (executed_at TIMESTAMP)
    USING DELTA
    """,
    args={"table_name": f"{namespace}.bootstrap_calls"},
).collect()
spark.sql(
    "INSERT INTO IDENTIFIER(:table_name) SELECT current_timestamp()",
    args={"table_name": f"{namespace}.bootstrap_calls"},
).collect()

# COMMAND ----------
display(spark.sql(
    "SELECT * FROM IDENTIFIER(:table_name)",
    args={"table_name": f"{namespace}.bootstrap_settings"},
))
display(spark.sql(
    "SELECT * FROM IDENTIFIER(:table_name) ORDER BY executed_at DESC",
    args={"table_name": f"{namespace}.bootstrap_calls"},
))
print("Initial setup completed. Successful unchanged deployments reuse this run.")
