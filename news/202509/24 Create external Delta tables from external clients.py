# Databricks notebook source
https://learn.microsoft.com/en-us/azure/databricks/external-access/create-external-tables

# COMMAND ----------

# MAGIC %sh
# MAGIC curl --location --request POST 'https://<workspace-url>/api/2.0/unity-catalog/tables/' \
# MAGIC --header 'Authorization: Bearer <token>' \
# MAGIC --header 'Content-Type: application/json' \
# MAGIC --data '{
# MAGIC   "name": "<table-name>",
# MAGIC   "catalog_name": "<uc-catalog-name>",
# MAGIC   "schema_name": "<schema-name>",
# MAGIC   "table_type": "EXTERNAL",
# MAGIC   "data_source_format": "DELTA",
# MAGIC   "storage_location": "<path>",
# MAGIC   "columns": [
# MAGIC     {
# MAGIC       "name": "id",
# MAGIC       "type_name": "LONG",
# MAGIC       "type_text": "bigint",
# MAGIC       "type_json": "\"long\"",
# MAGIC       "type_precision": 0,
# MAGIC       "type_scale": 0,
# MAGIC       "position": 0,
# MAGIC       "nullable": true
# MAGIC     },
# MAGIC     {
# MAGIC       "name": "name",
# MAGIC       "type_name": "STRING",
# MAGIC       "type_text": "string",
# MAGIC       "type_json": "\"string\"",
# MAGIC       "type_precision": 0,
# MAGIC       "type_scale": 0,
# MAGIC       "position": 1,
# MAGIC       "nullable": true
# MAGIC     }
# MAGIC   ]
# MAGIC }'
