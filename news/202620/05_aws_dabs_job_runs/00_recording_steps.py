# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Run jobs during bundle deployment
# MAGIC **Workspace:** aws-serverless-dev · **Compute:** serverless environment 6.
# MAGIC
# MAGIC Open each example's `resources/job.yml` beside its notebook. The code examples use a dedicated `main.news_202620_dabs` schema.
# MAGIC `job_runs` executes a referenced job as part of deployment; it requires the direct deployment engine.

# COMMAND ----------

# MAGIC %md
# MAGIC ## CLI workspace setup
# MAGIC Use the launcher from `C:\git\medium\news\202620` to select the correct config file, workspace profile and chapter directory:
# MAGIC ```powershell
# MAGIC .\demo.ps1 -Chapter 5 -Example 01_initial_setup -Action validate
# MAGIC .\demo.ps1 -Chapter 5 -Example 01_initial_setup -Action deploy
# MAGIC .\demo.ps1 -Chapter 5 -Example 01_initial_setup -Action summary
# MAGIC ```
# MAGIC For the direct CLI commands below, first set `$env:DATABRICKS_CONFIG_FILE = 'C:\git\cli\.databrickscfg'` and change into this chapter's bundle directory. Chapter 05 requires selecting an example subdirectory.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recording order
# MAGIC 1. `01_initial_setup`: deploy twice; a successful unchanged run is reused. Show the small bootstrap table and execution audit.
# MAGIC 2. `02_every_deployment`: point to `on_bundle_deploy: true`; deploy twice to show an additional audit row for every deployment.
# MAGIC 3. `03_json_configuration`: show `on_file_change` and `config/application.json`; change one synthetic configuration value and redeploy to update the table.
# MAGIC
# MAGIC Use `bundle deploy --profile DEFAULT` from the selected example folder. Deployment itself performs the demonstration; an additional Run now is unnecessary.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional validation examples
# MAGIC `04_validation_succeeds` uses an immutable snapshot and a validation run before a dependent job is deployed. `05_validation_fails` deliberately contains a negative multiplier and is expected to fail if deployed. They use separate bundle names.
# MAGIC
# MAGIC Immutable folders require workspace preview enablement. Use these examples only if that feature is available; the three core demonstrations do not require it. Do not describe a deliberately failed deployment as a platform failure.
# MAGIC
# MAGIC [Bundle job runs](https://docs.databricks.com/aws/en/dev-tools/bundles/resources#job_run) · [Immutable folders](https://docs.databricks.com/aws/en/dev-tools/bundles/reference#immutable_folder)
