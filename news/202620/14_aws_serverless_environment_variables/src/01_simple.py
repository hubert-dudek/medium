# Databricks notebook source
import os

app_env = os.environ["APP_ENV"]

log_level = os.getenv("LOG_LEVEL", "INFO")

print(f"APP_ENV = {app_env}")
print(f"LOG_LEVEL = {log_level}")
print(f"FILE_ONLY = {os.getenv('FILE_ONLY')}")

# COMMAND ----------

import json

dbutils.notebook.exit(json.dumps({
    "APP_ENV": app_env,
    "LOG_LEVEL": log_level,
    "FILE_ONLY": os.getenv("FILE_ONLY"),
}))
