# Databricks notebook source
# MAGIC %md
# MAGIC # Configurable system-table retention
# MAGIC
# MAGIC Workspace: AWS account associated with [the development workspace](https://dbc-ca93cffe-fba0.cloud.databricks.com/)
# MAGIC
# MAGIC Compute: none. Use the account console, not a SQL warehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record
# MAGIC
# MAGIC 1. Open the [AWS account console](https://accounts.cloud.databricks.com/) and confirm the account.
# MAGIC 2. Open **Settings** > **Account settings**.
# MAGIC 3. Locate **System Tables data retention (days)** and frame its existing value.
# MAGIC 4. Explain the range without entering a replacement or clicking **Save**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explain
# MAGIC
# MAGIC Configurable-retention Beta supports 30–3,650 days for supported system tables, with a 395-day default when enabled.
# MAGIC
# MAGIC Current documentation says retention is free during Beta; storage beyond 395 days becomes chargeable at general availability. Longer retention cannot recover already removed data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Availability and fallback
# MAGIC
# MAGIC An account admin and **System Tables Configurable Retention** preview are required; enablement can take up to 24 hours. Data-classification, data-quality-monitoring and Lakebase schemas are excluded.
# MAGIC
# MAGIC If the setting is absent, show the official retention section. Do not enable the preview or change retention to film it.
# MAGIC
# MAGIC [System-table retention](https://docs.databricks.com/aws/en/admin/system-tables#configure-system-table-data-retention)
