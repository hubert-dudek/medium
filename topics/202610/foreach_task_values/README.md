# For each task values: minimal demo

1. `generate_countries` publishes the hardcoded list `["Poland", "Czechia", "Italy"]` as a task value.
2. `process_countries` uses that list to run `02_process_country.py` once per country.

The nested notebook receives the current item through the `country` parameter:

```yaml
base_parameters:
  country: "{{input}}"
```

It reads and prints that value with a widget:

```python
dbutils.widgets.text("country", "")
print(dbutils.widgets.get("country"))
```

The three iterations print `Poland`, `Czechia`, and `Italy`, each in its own notebook output.

## Run

Requires a configured Databricks CLI profile and serverless Jobs compute.

```bash
cd topics/202610/foreach_task_values
databricks bundle validate -t dev -p DEFAULT
databricks bundle deploy -t dev -p DEFAULT
databricks bundle run countries_demo -t dev -p DEFAULT
```

[Databricks documentation: For each tasks](https://learn.microsoft.com/en-us/azure/databricks/jobs/tasks/for-each)
