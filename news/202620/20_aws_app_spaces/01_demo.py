# Databricks notebook source
# MAGIC %md
# MAGIC # App Spaces governance
# MAGIC
# MAGIC Workspace: [AWS serverless development](https://dbc-ca93cffe-fba0.cloud.databricks.com/)
# MAGIC
# MAGIC Compute: none. Inspect settings without deploying or running an app.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record
# MAGIC
# MAGIC 1. Open **Apps** > **App Spaces**.
# MAGIC 2. Open an existing demo space, if present.
# MAGIC 3. Frame **User authorization**, allowed scopes, **Serverless usage policy**, and **Permissions**.
# MAGIC 4. If no space exists, an authorized admin can open **+ New Space** to show the form, then cancel.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explain
# MAGIC
# MAGIC An App Space defines common rules for builders: who can create apps, which user-authorized scopes are allowed, and which usage policy applies.
# MAGIC
# MAGIC Permission to view a space does not automatically mean permission to create or use its apps. This differs from editor spaces for tabs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Availability and fallback
# MAGIC
# MAGIC App Spaces are Beta and require **Governed agentic app-building** in workspace Previews. Creation requires a workspace admin. Permissions include **Can Read**, **Can Create App** and **Can Manage**.
# MAGIC
# MAGIC If unavailable, show the documented example and call it a walkthrough. Leave scopes, policies and permissions unchanged.
# MAGIC
# MAGIC [App Spaces](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/app-spaces)
