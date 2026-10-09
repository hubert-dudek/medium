# Databricks notebook source
import json

dbutils.widgets.text("trigger_type", "")
dbutils.widgets.text("triggered_at", "")
dbutils.widgets.text("run_id", "")

# COMMAND ----------

result = {
    "trigger_type": dbutils.widgets.get("trigger_type"),
    "triggered_at": dbutils.widgets.get("triggered_at"),
    "run_id": dbutils.widgets.get("run_id"),
    "answer": 42,
}

print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
