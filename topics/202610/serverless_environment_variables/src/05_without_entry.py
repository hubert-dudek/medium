# Databricks notebook source
# MAGIC %md
# MAGIC # 5. A task without a selected entry
# MAGIC Leave Environment variables empty for this task. The job-level entry is not inherited.

# COMMAND ----------
import os

result = {key: os.getenv(key) for key in ("APP_ENV", "LOG_LEVEL", "FILE_ONLY", "ARTICLE_ENV_MARKER")}
print(result)
assert result["ARTICLE_ENV_MARKER"] is None
assert result["FILE_ONLY"] is None

# COMMAND ----------
import json

dbutils.notebook.exit(json.dumps(result))
