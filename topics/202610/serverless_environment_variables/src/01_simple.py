# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Read environment variables
# MAGIC Select `app_config` for this task in the job UI before running it.

# COMMAND ----------
import os

# Required: raises KeyError if the value is missing.
app_env = os.environ["APP_ENV"]

# Optional: use INFO when no LOG_LEVEL exists.
log_level = os.getenv("LOG_LEVEL", "INFO")

print(f"APP_ENV = {app_env}")
print(f"LOG_LEVEL = {log_level}")
print(f"FILE_ONLY = {os.getenv('FILE_ONLY')}")

# COMMAND ----------
# Structured output also makes the result easy to retrieve through the Jobs API.
import json

dbutils.notebook.exit(json.dumps({
    "APP_ENV": app_env,
    "LOG_LEVEL": log_level,
    "FILE_ONLY": os.getenv("FILE_ONLY"),
}))
