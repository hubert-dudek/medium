# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks news · 202620
# MAGIC Recording pack for **15 October 2026**. Follow chapter numbers: AWS first, then Azure.
# MAGIC
# MAGIC The pack covers the 24 news topics in the current article, plus serverless environment variables and automatic Change Data Feed. Code examples use small synthetic data. UI-only chapters contain recording steps and availability notes.
# MAGIC
# MAGIC [AWS workspace](https://dbc-ca93cffe-fba0.cloud.databricks.com/) · [Azure workspace](https://adb-7405607825310844.4.azuredatabricks.net/) · [News article](https://medium.com/databrickscommunity/databricks-news-serverless-genie-code-ltap-lakeflow-61853d8e422a)
# MAGIC
# MAGIC | Workspace | CLI profile | Recording compute |
# MAGIC |---|---|---|
# MAGIC | aws-serverless-dev | DEFAULT | Serverless environment 6; SQL warehouse for SQL results; serverless pipelines |
# MAGIC | azure-mvplink | AZURE-MVPLINK | Serverless for chapters 21, 22 and 26; single-node classic for chapter 23 |
# MAGIC
# MAGIC CLI: `C:\git\cli\databricks.exe` · Config: `C:\git\cli\.databrickscfg` · Checkout: `C:\git\medium`.
# MAGIC Environment version 6 is set in the jobs. For interactive notebooks, inspect the Environment panel before running; a job setting does not set an interactive notebook's environment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## AWS chapters
# MAGIC
# MAGIC | Chapter | Feature | Show |
# MAGIC |---|---|---|
# MAGIC | 01 | [Lakeflow Connect connectors](https://dbc-ca93cffe-fba0.cloud.databricks.com/editor/notebooks/942788509296563) | UI walkthrough |
# MAGIC | 02 | [Genie Code task](https://dbc-ca93cffe-fba0.cloud.databricks.com/editor/notebooks/942788509296565) | UI walkthrough |
# MAGIC | 03 | [Dynamic For each inputs](https://dbc-ca93cffe-fba0.cloud.databricks.com/editor/notebooks/942788509296569) | Runnable demo |
# MAGIC | 04 | [Resize streaming state](https://dbc-ca93cffe-fba0.cloud.databricks.com/editor/notebooks/942788509296572) | Runnable demo |
# MAGIC | 05 | [Run jobs during bundle deployment](https://dbc-ca93cffe-fba0.cloud.databricks.com/editor/notebooks/942788509296581) | CLI deployment examples |
# MAGIC | 06 | [Direct deployment engine](https://dbc-ca93cffe-fba0.cloud.databricks.com/editor/notebooks/942788509296615) | Runnable demo |
# MAGIC | 07 | [Run jobs and pipelines as a group](https://dbc-ca93cffe-fba0.cloud.databricks.com/editor/notebooks/942788509296620) | Runnable demo |
# MAGIC | 08 | [ai_decide](https://dbc-ca93cffe-fba0.cloud.databricks.com/editor/notebooks/942788509296628) | Runnable demo |
# MAGIC | 09 | [counter_diff](https://dbc-ca93cffe-fba0.cloud.databricks.com/editor/notebooks/942788509296632) | Runnable demo |
# MAGIC | 10 | [parse_sql](https://dbc-ca93cffe-fba0.cloud.databricks.com/editor/notebooks/942788509296636) | Runnable demo |
# MAGIC | 11 | [truncate](https://dbc-ca93cffe-fba0.cloud.databricks.com/editor/notebooks/942788509296642) | Runnable demo |
# MAGIC | 12 | [trim_array](https://dbc-ca93cffe-fba0.cloud.databricks.com/editor/notebooks/942788509296646) | Runnable demo |
# MAGIC | 13 | [Serverless environment 6](https://dbc-ca93cffe-fba0.cloud.databricks.com/editor/notebooks/942788509296648) | UI walkthrough |
# MAGIC | 14 | [Serverless environment variables](https://dbc-ca93cffe-fba0.cloud.databricks.com/editor/notebooks/942788509296654) | Runnable demo |
# MAGIC | 15 | [Skills in Unity Catalog](https://dbc-ca93cffe-fba0.cloud.databricks.com/editor/notebooks/942788509296661) | UI walkthrough |
# MAGIC | 16 | [External model cost tracking](https://dbc-ca93cffe-fba0.cloud.databricks.com/editor/notebooks/942788509296663) | UI walkthrough |
# MAGIC | 17 | [Notebook tabs and spaces](https://dbc-ca93cffe-fba0.cloud.databricks.com/editor/notebooks/942788509296665) | UI walkthrough |
# MAGIC | 18 | [Serverless governance](https://dbc-ca93cffe-fba0.cloud.databricks.com/editor/notebooks/942788509296667) | UI walkthrough |
# MAGIC | 19 | [System table retention](https://dbc-ca93cffe-fba0.cloud.databricks.com/editor/notebooks/942788509296669) | UI walkthrough |
# MAGIC | 20 | [App Spaces](https://dbc-ca93cffe-fba0.cloud.databricks.com/editor/notebooks/942788509296671) | UI walkthrough |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Azure chapters
# MAGIC
# MAGIC | Chapter | Feature | Show |
# MAGIC |---|---|---|
# MAGIC | 21 | [Multiple job triggers](https://adb-7405607825310844.4.azuredatabricks.net/editor/notebooks/2091443487632006) | Runnable demo |
# MAGIC | 22 | [Nanosecond timestamps](https://adb-7405607825310844.4.azuredatabricks.net/editor/notebooks/2091443487632012) | Runnable demo |
# MAGIC | 23 | [Environments on classic compute](https://adb-7405607825310844.4.azuredatabricks.net/editor/notebooks/2091443487632018) | Runnable demo |
# MAGIC | 24 | [Unity Catalog secrets backed by Key Vault](https://adb-7405607825310844.4.azuredatabricks.net/editor/notebooks/2091443487632021) | UI walkthrough |
# MAGIC | 25 | [Private Network Gateway](https://adb-7405607825310844.4.azuredatabricks.net/editor/notebooks/2091443487632023) | UI walkthrough |
# MAGIC | 26 | [Automatic Change Data Feed](https://adb-7405607825310844.4.azuredatabricks.net/editor/notebooks/2091443487632027) | Runnable demo |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepared workspace resources
# MAGIC
# MAGIC These links open the isolated recording resources. Use the latest completed run to show output without starting compute.
# MAGIC
# MAGIC | Chapter | Resource |
# MAGIC |---|---|
# MAGIC | 03 · Dynamic For each inputs | [demo](https://dbc-ca93cffe-fba0.cloud.databricks.com/jobs/359923813521646?w=3665629714053906) |
# MAGIC | 04 · Resize streaming state | [standalone](https://dbc-ca93cffe-fba0.cloud.databricks.com/jobs/721518332401071?w=3665629714053906) |
# MAGIC | 04 · Resize streaming state | [state_repartitioning](https://dbc-ca93cffe-fba0.cloud.databricks.com/pipelines/36b78fba-d8bc-4acc-a20c-52bcf400175a?w=3665629714053906) |
# MAGIC | 05 · Run jobs during bundle deployment | [initialize_job](https://dbc-ca93cffe-fba0.cloud.databricks.com/jobs/4034963624707?w=3665629714053906) |
# MAGIC | 05 · Run jobs during bundle deployment | [record_deployment_job](https://dbc-ca93cffe-fba0.cloud.databricks.com/jobs/748630678198562?w=3665629714053906) |
# MAGIC | 05 · Run jobs during bundle deployment | [merge_configuration_job](https://dbc-ca93cffe-fba0.cloud.databricks.com/jobs/842406245673917?w=3665629714053906) |
# MAGIC | 05 · Run jobs during bundle deployment | [production_job](https://dbc-ca93cffe-fba0.cloud.databricks.com/jobs/659262525198489?w=3665629714053906) |
# MAGIC | 05 · Run jobs during bundle deployment | [validation_job](https://dbc-ca93cffe-fba0.cloud.databricks.com/jobs/476450633112397?w=3665629714053906) |
# MAGIC | 06 · Direct deployment engine | [hello](https://dbc-ca93cffe-fba0.cloud.databricks.com/jobs/164980652518510?w=3665629714053906) |
# MAGIC | 07 · Run jobs and pipelines as a group | [group_job](https://dbc-ca93cffe-fba0.cloud.databricks.com/jobs/343261329678278?w=3665629714053906) |
# MAGIC | 07 · Run jobs and pipelines as a group | [user_override](https://dbc-ca93cffe-fba0.cloud.databricks.com/jobs/743995497281638?w=3665629714053906) |
# MAGIC | 07 · Run jobs and pipelines as a group | [group_pipeline](https://dbc-ca93cffe-fba0.cloud.databricks.com/pipelines/245062dd-e9da-43e5-94ec-3b0b7915668e?w=3665629714053906) |
# MAGIC | 08 · ai_decide | [demo](https://dbc-ca93cffe-fba0.cloud.databricks.com/jobs/52293383160907?w=3665629714053906) |
# MAGIC | 09 · counter_diff | [demo](https://dbc-ca93cffe-fba0.cloud.databricks.com/jobs/287776711264169?w=3665629714053906) |
# MAGIC | 10 · parse_sql | [demo](https://dbc-ca93cffe-fba0.cloud.databricks.com/jobs/153619445864304?w=3665629714053906) |
# MAGIC | 11 · truncate | [demo](https://dbc-ca93cffe-fba0.cloud.databricks.com/jobs/368166132797949?w=3665629714053906) |
# MAGIC | 12 · trim_array | [demo](https://dbc-ca93cffe-fba0.cloud.databricks.com/jobs/99428553310408?w=3665629714053906) |
# MAGIC | 14 · Serverless environment variables | [env_demo](https://dbc-ca93cffe-fba0.cloud.databricks.com/jobs/758034753049399?w=3665629714053906) |
# MAGIC | 21 · Multiple job triggers | [demo](https://adb-7405607825310844.4.azuredatabricks.net/jobs/62161337285226) |
# MAGIC | 22 · Nanosecond timestamps | [demo](https://adb-7405607825310844.4.azuredatabricks.net/jobs/161233138918707) |
# MAGIC | 23 · Environments on classic compute | [demo](https://adb-7405607825310844.4.azuredatabricks.net/jobs/1019594239405061) |
# MAGIC | 26 · Automatic Change Data Feed | [demo](https://adb-7405607825310844.4.azuredatabricks.net/jobs/984379516276542) |
# MAGIC | 21 · Multiple job triggers | [Editable copy for filming the trigger dialog](https://adb-7405607825310844.4.azuredatabricks.net/jobs/538508346158882) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run one chapter
# MAGIC Open **PowerShell 7** in `C:\git\medium\news\202620`:
# MAGIC ```powershell
# MAGIC .\demo.ps1
# MAGIC .\demo.ps1 -Chapter 11 -Action validate
# MAGIC .\demo.ps1 -Chapter 11 -Action deploy
# MAGIC .\demo.ps1 -Chapter 11 -Action run
# MAGIC .\demo.ps1 -Chapter 21 -Action summary
# MAGIC .\demo.ps1 -Chapter 5 -Example 02_every_deployment -Action deploy
# MAGIC .\demo.ps1 -Chapter 4 -Action pipeline
# MAGIC ```
# MAGIC The launcher selects the chapter's CLI profile and checks the account before performing an action. `deploy` creates or updates only that chapter's bundle. Chapter 05 runs its demonstration during deployment. Chapter 23 uses its own wrapper to stop classic compute after a run.
# MAGIC
# MAGIC For a UI-only chapter, the launcher prints the walkthrough notebook path. Open that notebook in the matching workspace. No account-level preview, credential, networking or permission change is needed simply to present the walkthrough.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recording details
# MAGIC - Start code demos from the first numbered cell. SQL source files are notebooks with cell separators, not a single long query.
# MAGIC - Use `bundle summary` or the job's latest completed run for results. Show actual outputs; AI choices may differ between runs.
# MAGIC - Chapter 04 retains checkpoint state; repeating the controller adds another 16 unique events. Its SQL results are read from the pipeline's `event_log`.
# MAGIC - Chapter 07 reuses the existing authorized demo group and schema. Use only one of its deployment targets at a time.
# MAGIC - Chapter 21 has three triggers configured and paused. Its setup-tables notebook is a prerequisite when deploying into a fresh namespace. Its manual run illustrates the task; it does not claim a table-update trigger has fired. Keep triggers paused after filming.
# MAGIC - Chapter 22 is assigned to Azure because nanosecond support is workspace-dependent. Its simple query compares the same input at six and nine fractional digits.
# MAGIC - Chapter 23 needs DBR 19+, standard access and dependency mode Environments. The existing single-node cluster has a 10-minute idle timeout; stop it immediately after the demonstration.
# MAGIC - Chapter 25 is a documentation walkthrough because Private Network Gateway has no creation UI in the current preview. Key Vault, gateway and external-provider credentials are not created by this pack.
# MAGIC - Chapter 26 recreates only its two small synthetic tables, `main.news_202620_automatic_cdf.classic_cdf` and `main.news_202620_automatic_cdf.automatic_cdf`, when rerun.
# MAGIC - `04_validation_succeeds` and `05_validation_fails` under chapter 05 are optional immutable-folder preview examples. The failing example is intentionally negative; it is not part of the normal recording run.
# MAGIC
# MAGIC After filming, leave demo jobs unscheduled and stop only demo compute started for this recording. Preserve warehouses or clusters that were already in use for other work. Keep demo data and existing successful runs so you can revisit results.
