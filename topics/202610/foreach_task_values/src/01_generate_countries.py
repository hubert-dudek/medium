# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 1. Publish the country list
# MAGIC The job parameter is JSON text. Parse it and publish an actual Python list.
# MAGIC The next task uses this list as its `For each` input.

# COMMAND ----------

countries = ["Poland", "Czechia", "Italy"]
dbutils.jobs.taskValues.set(key="countries", value=countries)