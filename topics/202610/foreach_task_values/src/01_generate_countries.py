# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 1. Publish the country list
# MAGIC The optional examples are commented out. The final cell publishes the hardcoded country list.
# MAGIC The next task uses this list as its `For each` input.

# COMMAND ----------

# Optional examples: these are commented out and are not used by this demo.

# Pass a string.
# dbutils.jobs.taskValues.set(key="status", value="ready")

# Pass a number.
# dbutils.jobs.taskValues.set(key="row_count", value=100)

# Pass a boolean.
# dbutils.jobs.taskValues.set(key="has_data", value=True)

# Pass a dictionary (a JSON-compatible object).
# config = {"country": "Poland", "currency": "PLN", "full_refresh": False}
# dbutils.jobs.taskValues.set(key="config", value=config)

# Pass a list of dictionaries for another For each example.
# country_configs = [
#     {"country": "Poland", "currency": "PLN"},
#     {"country": "Italy", "currency": "EUR"},
# ]
# dbutils.jobs.taskValues.set(key="country_configs", value=country_configs)
# In that For each, use {{tasks.generate_countries.values.country_configs}}
# as inputs, then pass {{input.country}} or {{input.currency}} to its task.

# Calculate a value before passing it.
# country_count = len(["Poland", "Czechia", "Italy"])
# dbutils.jobs.taskValues.set(key="country_count", value=country_count)

# Read a value in a DOWNSTREAM task, after enabling the config example above.
# config = dbutils.jobs.taskValues.get(taskKey="generate_countries", key="config")
# print(config["country"])

# COMMAND ----------

countries = ["Poland", "Czechia", "Italy"]
dbutils.jobs.taskValues.set(key="countries", value=countries)
