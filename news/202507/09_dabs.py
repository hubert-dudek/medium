# Databricks notebook source
pipeline_spec = """
{
 "name": "<YOUR_PIPELINE_NAME>",
 "ingestion_definition": {
     "connection_name": "<YOUR_CONNECTON_NAME>",
     "objects": [
        {
          "table": {
            "source_schema": "<YOUR_SHAREPOINT_SITE_ID>", 
            "source_table": "<YOUR_SHAREPOINT_DRIVE_NAME>",
            "destination_catalog": "<YOUR_DATABRICKS_CATALOG>",
            "destination_schema": "<YOUR_DATABRICKS_SCHEMA>",
            "destination_table": "<NAME"> # e.g., "my_drive",
            "table_configuration": {
              "scd_type": "SCD_TYPE_1"
            }
          }
        }
      ]
 },
 "channel": "PREVIEW"
}
"""

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks pipelines create --json "<pipeline definition or json file path>"

# COMMAND ----------

# MAGIC %md
# MAGIC build knowlege base
