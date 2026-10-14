# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Code as a Lakeflow task
# MAGIC
# MAGIC Workspace: [AWS serverless development](https://dbc-ca93cffe-fba0.cloud.databricks.com/)
# MAGIC
# MAGIC Compute: none for this configuration tour. Do not execute the agent task.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record
# MAGIC
# MAGIC 1. Open an existing demo job in **Jobs & Pipelines** > **Tasks**.
# MAGIC 2. Choose **Add task** and select **Genie Code** from **Type**, if available.
# MAGIC 3. Show the **Prompt** field. Suggested text: “Summarize the upstream demo task output. Do not modify data or contact external services.”
# MAGIC 4. Explain a job parameter reference such as `{{region}}`, then cancel the unsaved task.
# MAGIC 5. Only if a completed demo run already exists, show its conversation link and response.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explain
# MAGIC
# MAGIC A natural-language agent can participate in the job graph, inspect upstream task output and return a conversation that can be continued later.
# MAGIC
# MAGIC Unlike a scheduled interactive Genie Code chat, this runs as a job task. Auto-approval is always on, so the unattended prompt and accessible resources matter.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Availability and fallback
# MAGIC
# MAGIC The task type is Beta and requires Genie Code agentic capabilities. If absent from **Type**, show the documented configuration and output sections instead. Do not imply that a task was executed.
# MAGIC
# MAGIC [Genie Code task](https://docs.databricks.com/aws/en/jobs/tasks/genie-code)
