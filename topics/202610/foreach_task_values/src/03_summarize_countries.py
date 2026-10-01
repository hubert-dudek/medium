# Databricks notebook source
# MAGIC %md
# MAGIC # 3. Read every iteration's output and loop in Python
# MAGIC **The new feature:** reading the nested task's value returns a list of all
# MAGIC iteration outputs, in input order. Concurrent completion order does not
# MAGIC change this order. A missing key becomes `None` at that iteration's index.

# COMMAND ----------

import json

# Use the nested task key (process_country), not the outer process_countries.
# No default/debugValue: running this as a job must read real upstream outputs.
results = dbutils.jobs.taskValues.get(taskKey="process_country", key="result")
print("All iteration outputs:")
print(json.dumps(results, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## The same outputs through a task parameter
# MAGIC The bundle passes `{{tasks.process_country.values.result}}` as
# MAGIC `results_json`. Databricks recommends this parameter-based approach
# MAGIC because the notebook does not need to know the upstream task name.

# COMMAND ----------

dbutils.widgets.text("results_json", "")
dbutils.widgets.text("input_countries_json", "")
results_from_parameter = json.loads(dbutils.widgets.get("results_json"))
countries = json.loads(dbutils.widgets.get("input_countries_json"))

assert isinstance(results, list), "Expected the aggregated iteration output list."
assert results == results_from_parameter, "The two read methods should return the same values."
assert len(results) == len(countries), "Each input should retain its position in the output."
print("taskValues.get() and the dynamic parameter return the same list.")

# COMMAND ----------

total_rows = 0
missing_countries = []

# This is a normal Python loop in ONE downstream task.
for country, result in zip(countries, results):
    if result is None:
        missing_countries.append(country)
        print(f"{country}: no result (None)")
        continue

    assert result["country"] == country, "Outputs should follow the input order."
    total_rows += result["rows_processed"]
    print(f"{country}: {result['rows_processed']} rows")

print(f"Total rows: {total_rows}")
print(f"Missing results: {missing_countries}")

dbutils.jobs.taskValues.set(
    key="summary",
    value={"total_rows": total_rows, "missing_countries": missing_countries},
)
