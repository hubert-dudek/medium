# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Connect connectors
# MAGIC
# MAGIC Workspace: [AWS serverless development](https://dbc-ca93cffe-fba0.cloud.databricks.com/)
# MAGIC
# MAGIC Compute: none for this UI tour. Inspect the source catalog and setup wizard without running ingestion.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record
# MAGIC
# MAGIC 1. Open **Data Ingestion** in the sidebar.
# MAGIC 2. On **Add data**, frame **Databricks connectors** and search for OpenAI, Anthropic, Notion, Gmail, PagerDuty and SendGrid.
# MAGIC 3. Show the actual release labels. Open **OpenAI** and frame the **Connection** step.
# MAGIC 4. Stop before creating a connection or pipeline. Return to the catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explain
# MAGIC
# MAGIC OpenAI ingestion imports organization data such as usage, costs and audit logs; it is different from calling an AI model.
# MAGIC
# MAGIC Describe the catalog visible on recording day. Do not label every connector in the grid as newly released or equate a missing partner tile with removal of a product.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Availability and fallback
# MAGIC
# MAGIC OpenAI ingestion is Beta and needs its workspace preview, Unity Catalog, serverless and source/destination privileges. Other connectors have their own release states.
# MAGIC
# MAGIC If a card or wizard is missing, show the supported-source documentation and label the segment a walkthrough. This chapter does not test a source connection.
# MAGIC
# MAGIC [Connector concepts](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/) · [SaaS list](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/saas-overview) · [OpenAI setup](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/openai-pipeline)
