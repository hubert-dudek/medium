# Databricks notebook source
# MAGIC %md
# MAGIC # Environment available before import
# MAGIC This module uses only Python's standard environment API.

# COMMAND ----------
import json
import app_settings

result = {
    "app_env": app_settings.APP_ENV,
    "log_level": app_settings.LOG_LEVEL,
    "file_only": app_settings.FILE_ONLY,
}
print(json.dumps(result, indent=2))
assert app_settings.APP_ENV != "from-file", "Inline APP_ENV should override the file"
assert app_settings.FILE_ONLY == "loaded-from-file"

# COMMAND ----------
dbutils.notebook.exit(json.dumps(result))
