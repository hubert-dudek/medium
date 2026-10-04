# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Keep settings in one module
# MAGIC `app_settings.py` is our own file beside this notebook, not a Databricks library.

# COMMAND ----------
import app_settings

print(f"APP_ENV = {app_settings.APP_ENV}")
print(f"LOG_LEVEL = {app_settings.LOG_LEVEL}")
print(f"FILE_ONLY = {app_settings.FILE_ONLY}")

# COMMAND ----------
# A normal function can use the same shared configuration.
def describe_export():
    return f"Preparing an export for {app_settings.APP_ENV}"

print(describe_export())

# COMMAND ----------
import json

dbutils.notebook.exit(json.dumps({
    "APP_ENV": app_settings.APP_ENV,
    "LOG_LEVEL": app_settings.LOG_LEVEL,
    "FILE_ONLY": app_settings.FILE_ONLY,
}))
