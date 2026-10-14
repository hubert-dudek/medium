# Databricks notebook source
dbutils.widgets.text("country", "")
country = dbutils.widgets.get("country")
assert country in ["Poland", "Czechia", "Italy"]

# COMMAND ----------

display(spark.createDataFrame([(country, "processed")], ["country", "status"]))

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({"country": country, "status": "processed"}))
