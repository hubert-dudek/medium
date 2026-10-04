# Databricks notebook source
# MAGIC %md
# MAGIC # No entry selected
# MAGIC Defining a job entry does not attach it to every task.

# COMMAND ----------
import json
import os

result = {name: os.getenv(name) for name in ("APP_ENV", "LOG_LEVEL", "FILE_ONLY", "ARTICLE_ENV_MARKER")}
print(json.dumps(result, indent=2))
assert result["ARTICLE_ENV_MARKER"] is None, "The unconfigured task received the demo entry"
assert result["FILE_ONLY"] is None, "The unconfigured task received the demo file"

dbutils.notebook.exit(json.dumps(result))
