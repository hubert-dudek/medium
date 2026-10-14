# Databricks notebook source
# MAGIC %md
# MAGIC # Serverless environment version 6
# MAGIC
# MAGIC Workspace: [AWS serverless development](https://dbc-ca93cffe-fba0.cloud.databricks.com/)
# MAGIC
# MAGIC Compute: UI only. Do not run cells or start a new notebook session.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record
# MAGIC
# MAGIC 1. Open a demo notebook and show its compute selector.
# MAGIC 2. Open its **Environment** side panel.
# MAGIC 3. Inspect **Base environment** and the version shown for Standard v6. Read the selected value; existing notebooks need not match the workspace default.
# MAGIC 4. Show the library/dependency information, then close without applying another environment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explain
# MAGIC
# MAGIC Environment versions fix the client API and library set while the server can upgrade independently. Version 6 includes Databricks Connect 19 and Python 3.12.3.
# MAGIC
# MAGIC Say “environment version 6,” not “the server is Runtime 19.” SQL warehouses have independent engine versions and channels.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Availability and fallback
# MAGIC
# MAGIC If serverless or the panel is unavailable, show the environment-version table and base-environment documentation. A configuration display is not an execution test.
# MAGIC
# MAGIC [Environment versions](https://docs.databricks.com/aws/en/release-notes/serverless/environment-version) · [Environment panel](https://docs.databricks.com/aws/en/compute/serverless/dependencies)
