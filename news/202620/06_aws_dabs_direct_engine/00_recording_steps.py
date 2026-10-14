# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Bundles use the direct deployment engine
# MAGIC **Workspace:** aws-serverless-dev · **Compute:** serverless environment 6 for the optional one-row job.
# MAGIC
# MAGIC Open `databricks.yml` and highlight `bundle.engine: direct`. Databricks CLI 1.20 removed the Terraform deployment engine from bundles. This does not remove Terraform as a separate Databricks infrastructure tool.

# COMMAND ----------

# MAGIC %md
# MAGIC ## CLI workspace setup
# MAGIC Use the launcher from `C:\git\medium\news\202620` to select the correct config file, workspace profile and chapter directory:
# MAGIC ```powershell
# MAGIC .\demo.ps1 -Chapter 6 -Action validate
# MAGIC .\demo.ps1 -Chapter 6 -Action deploy
# MAGIC .\demo.ps1 -Chapter 6 -Action summary
# MAGIC ```
# MAGIC For the direct CLI commands below, first set `$env:DATABRICKS_CONFIG_FILE = 'C:\git\cli\.databrickscfg'` and change into this chapter's bundle directory. Chapter 05 requires selecting an example subdirectory.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show the deployment and its state
# MAGIC ```powershell
# MAGIC & C:\git\cli\databricks.exe --version
# MAGIC & C:\git\cli\databricks.exe bundle validate --profile DEFAULT
# MAGIC & C:\git\cli\databricks.exe bundle plan --profile DEFAULT
# MAGIC & C:\git\cli\databricks.exe bundle deploy --profile DEFAULT
# MAGIC & C:\git\cli\databricks.exe bundle summary --profile DEFAULT
# MAGIC ```
# MAGIC Open the bundle's workspace folder, then `dev/state/resources.json`. Show the resource key and remote job ID. Keep generated deployment state outside Git.
# MAGIC Optionally run `bundle run hello --profile DEFAULT` to show the one-row notebook result.
# MAGIC
# MAGIC [Direct deployment engine](https://docs.databricks.com/aws/en/dev-tools/bundles/direct)
