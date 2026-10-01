# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Publish one result per iteration
# MAGIC Lakeflow Jobs runs this notebook once per country, with up to three
# MAGIC iterations at a time. Each iteration sets the same task-value key.
# MAGIC The row counts below are invented demo data; no tables are read or written.

# COMMAND ----------

import json

dbutils.widgets.text("country", "")
dbutils.widgets.text("skip_result_for", "")
country = dbutils.widgets.get("country")
skip_result_for = dbutils.widgets.get("skip_result_for")

demo_row_counts = {
    "Poland": 120,
    "Czechia": 80,
    "Italy": 150,
    "Germany": 200,
    "Spain": 90,
}

if country not in demo_row_counts:
    raise ValueError(f"Unknown demo country {country!r}. Choose from {list(demo_row_counts)}.")

# COMMAND ----------

# Optional demonstration: succeed without setting the key for one country.
# This is different from a failed iteration, which fails the For each task.
if country == skip_result_for:
    print(f"{country}: intentionally leaving the result task value unset.")
else:
    result = {"country": country, "rows_processed": demo_row_counts[country]}
    dbutils.jobs.taskValues.set(key="result", value=result)
    print(json.dumps(result, indent=2))
