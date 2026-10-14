# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Run jobs and pipelines as a group
# MAGIC **Workspace:** aws-serverless-dev · **Compute:** serverless environment 6 and a serverless pipeline.
# MAGIC
# MAGIC Use the existing account group `medium-run-as-demo`. Show the top-level `run_as.group_name` and the target-level variant in `databricks.yml`.
# MAGIC The group's own workspace, source-code and data permissions determine access; members' personal permissions are not inherited.

# COMMAND ----------

# MAGIC %md
# MAGIC ## CLI workspace setup
# MAGIC Use the launcher from `C:\git\medium\news\202620` to select the correct config file, workspace profile and chapter directory:
# MAGIC ```powershell
# MAGIC .\demo.ps1 -Chapter 7 -Action validate
# MAGIC .\demo.ps1 -Chapter 7 -Action deploy
# MAGIC .\demo.ps1 -Chapter 7 -Action summary
# MAGIC ```
# MAGIC For the direct CLI commands below, first set `$env:DATABRICKS_CONFIG_FILE = 'C:\git\cli\.databrickscfg'` and change into this chapter's bundle directory. Chapter 05 requires selecting an example subdirectory.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recording sequence
# MAGIC 1. Deploy the default `bundle_level` target.
# MAGIC 2. Run `group_job`; show `current_user()` returning the group and the answer 42.
# MAGIC 3. Run `group_pipeline`; show Run as in pipeline details and the materialized view.
# MAGIC 4. Optionally run `user_override` to compare the explicit user identity.
# MAGIC
# MAGIC ```powershell
# MAGIC & C:\git\cli\databricks.exe bundle deploy -t bundle_level --profile DEFAULT
# MAGIC & C:\git\cli\databricks.exe bundle run group_job -t bundle_level --profile DEFAULT
# MAGIC & C:\git\cli\databricks.exe bundle run group_pipeline -t bundle_level --profile DEFAULT
# MAGIC ```
# MAGIC The pipeline writes only `workspace.medium_run_as_group_bundle_level.news_202620_group_result`, inside the schema already authorized for this group.
# MAGIC Validate `target_level` to show the alternative; do not deploy both targets concurrently because they use the same demonstration output table.
