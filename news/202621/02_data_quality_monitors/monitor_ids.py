# Databricks notebook source
from databricks.sdk import WorkspaceClient

workspace = WorkspaceClient()
catalog = spark.catalog.currentCatalog()
schema = workspace.schemas.get(f"{catalog}.news_202621_quality")
table = workspace.tables.get(f"{schema.full_name}.sales")

# COMMAND ----------

print(f"table_id = {table.table_id}")
print(f"output_schema_id = {schema.schema_id}")

# COMMAND ----------

print(f"databricks bundle validate --var table_id={table.table_id},output_schema_id={schema.schema_id}")
