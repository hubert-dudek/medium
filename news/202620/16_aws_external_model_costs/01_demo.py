# Databricks notebook source
# MAGIC %md
# MAGIC # External model cost visibility
# MAGIC
# MAGIC Workspace: [AWS serverless development](https://dbc-ca93cffe-fba0.cloud.databricks.com/)
# MAGIC
# MAGIC Compute: none for setup inspection. Do not send model requests or start a warehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record
# MAGIC
# MAGIC 1. Open **Unity Gateway** or **AI Gateway**, using the label shown in this workspace.
# MAGIC 2. Open **Govern** > **Usage Dashboard** > **Cost Analysis** or **Cost Observability**, if already available.
# MAGIC 3. Frame existing cost breakdowns by model service, target model and requester.
# MAGIC 4. If a refresh would start compute, stop and show the documented dashboard example instead.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explain
# MAGIC
# MAGIC For traffic through Unity Catalog model-provider services, Databricks estimates external costs from token use and provider prices. Hourly records appear in `system.ai_gateway.external_model_spend`.
# MAGIC
# MAGIC These estimates are not the provider's invoice. Explain cost attribution separately from budget and enforcement controls.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Availability and fallback
# MAGIC
# MAGIC Cost observability needs an appropriate dashboard version and access. External-cost tracking is for Unity Catalog model-provider services; custom-provider pricing is unsupported.
# MAGIC
# MAGIC If no dashboard or traffic exists, show the documented schema/example rather than fabricating spend or creating paid traffic.
# MAGIC
# MAGIC [Cost observability](https://docs.databricks.com/aws/en/ai-gateway/cost-observability) · [Usage dashboard](https://docs.databricks.com/aws/en/ai-gateway/usage-tracking)
