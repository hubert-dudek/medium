# Databricks notebook source
# MAGIC %md
# MAGIC # Serverless compute access controls
# MAGIC
# MAGIC Workspace: [AWS serverless development](https://dbc-ca93cffe-fba0.cloud.databricks.com/)
# MAGIC
# MAGIC Compute: none. Inspect existing permission boundaries without changing access.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record
# MAGIC
# MAGIC 1. Open **Compute** > **Serverless**.
# MAGIC 2. Frame **Default Interactive Compute** and **Default Automated Compute**.
# MAGIC 3. Open each object's kebab menu > **Edit permissions**, if authorized.
# MAGIC 4. Point out **Can Use** and **Can Manage**, then close without editing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explain
# MAGIC
# MAGIC The interactive object governs notebooks and Databricks Connect; the automated object governs jobs and pipelines. **Can Use** permits workloads; **Can Manage** also permits permission administration.
# MAGIC
# MAGIC These controls do not cover all serverless products: SQL warehouses, model serving and Apps have separate controls. They are not an absolute spending cap.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Availability and fallback
# MAGIC
# MAGIC Access to the controls depends on workspace permissions. If the tab or dialog is unavailable, show the documented default objects and supported-feature list.
# MAGIC
# MAGIC No grants, revocations or compute objects are changed in this chapter.
# MAGIC
# MAGIC [Manage serverless compute](https://docs.databricks.com/aws/en/compute/serverless/manage-serverless-compute)
