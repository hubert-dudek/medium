# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Publish the country list
# MAGIC The job parameter is JSON text. Parse it and publish an actual Python list.
# MAGIC The next task uses this list as its `For each` input.

# COMMAND ----------

import json

dbutils.widgets.text("countries_json", "")
countries = json.loads(dbutils.widgets.get("countries_json"))

if not isinstance(countries, list) or not countries:
    raise ValueError("countries_json must be a non-empty JSON array of country names.")
if any(not isinstance(country, str) or not country.strip() for country in countries):
    raise ValueError("Each country must be a non-empty string.")

# Pass the list itself, not json.dumps(countries).
dbutils.jobs.taskValues.set(key="countries", value=countries)
print(f"Countries sent to For each: {countries}")
