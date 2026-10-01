# For each task values: minimal demo

Three small Python notebooks. No widgets, job parameters, or manual inputs.

1. `generate_countries` publishes the hardcoded list `["Poland", "Czechia", "Italy"]`.
2. `process_countries` runs the nested `process_country` notebook once per list item.
   Each iteration publishes the hardcoded task value `result = "done"`.
3. `summarize_countries` reads the aggregated results and loops over them in Python.

The nested notebook does not read or process a country; it only emits a fixed
result. The country list controls the iterations. The YAML `inputs` field is
required For each wiring, with its value supplied automatically by the first task.

## The new functionality

```python
results = dbutils.jobs.taskValues.get(taskKey="process_country", key="result")
# ["done", "done", "done"]
```

Values from all iterations arrive as one list in input order. The final notebook
pairs that list with the original countries:

```python
for country, result in zip(countries, results):
    print(f"{country}: {result}")
```

Expected final notebook output:

```text
['done', 'done', 'done']
Poland: done
Czechia: done
Italy: done
```

The downstream dependency uses the outer task `process_countries`.
The task-value read uses the nested task `process_country`.

## Run

Requires a configured Databricks CLI profile and serverless Jobs compute.

```bash
cd topics/202610/foreach_task_values
databricks bundle validate -t dev -p DEFAULT
databricks bundle deploy -t dev -p DEFAULT
databricks bundle run countries_demo -t dev -p DEFAULT
```

Run the job to see the task values passed between notebooks. Python/YAML checks
can run locally; aggregation itself requires a Databricks workspace run.

[Databricks documentation: read For each iteration outputs](https://learn.microsoft.com/en-us/azure/databricks/jobs/task-values#read-values-from-a-for-each-tasks-iterations)
