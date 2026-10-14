# Databricks notebook source
# MAGIC %md
# MAGIC # Three triggers for one job
# MAGIC
# MAGIC On the first run, open `00_setup_tables` in this chapter, select serverless environment 6, and run all cells before deploying the bundle. This creates only two tiny tables in `main.news_202620_multiple_triggers`.

# COMMAND ----------

# MAGIC %md
# MAGIC Deploy this chapter to Azure with the `AZURE-MVPLINK` profile. Open its job and show **Schedules & triggers (3)** next to `databricks.yml`: two UTC schedules and one table-update trigger watching both demo tables.
# MAGIC
# MAGIC `ALL_UPDATED` waits until both watched tables have changed. `ANY_UPDATED` would react when either watched table changes. These conditions belong to the table-update trigger; the three job triggers are independent alternatives.

# COMMAND ----------

# MAGIC %md
# MAGIC Keep all three triggers paused. Run the job manually and show its trigger type, time, run ID, and answer.
# MAGIC
# MAGIC The bundle-managed job is locked for UI edits. For the edit dialog, open the prepared copy [news-202620-multiple-triggers-ui-edit](https://adb-7405607825310844.4.azuredatabricks.net/jobs/538508346158882), inspect All tables updated versus Any table updated, and cancel without saving. All triggers must remain paused. A manual run demonstrates the task, not an automatic trigger firing.
