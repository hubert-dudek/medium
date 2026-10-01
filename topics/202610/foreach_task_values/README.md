# For each task values: countries in, results out

A small DAB example for presenting the new Lakeflow Jobs feature: collect the
task values set by every `For each` iteration, then loop over them in Python.

Passing a list **into** `For each` was already supported. The new part is reading
the iteration **outputs** as one ordered list in a downstream task.

## Run it

Requirements: a current Databricks CLI with bundle support, a configured workspace
authentication profile, and serverless Jobs compute. The documented aggregation
feature is Beta, enabled by default, and requires Databricks Runtime 15.4 LTS or
newer on classic compute. This bundle uses serverless, whose runtime is managed
by Databricks.

From your local checkout of this repository:

```bash
cd topics/202610/foreach_task_values
databricks bundle validate -t dev -p DEFAULT
databricks bundle deploy -t dev -p DEFAULT
databricks bundle run countries_demo -t dev -p DEFAULT
```

Replace `DEFAULT` with your workspace profile if needed. The development target
adds your user prefix to the job name. Deploying creates the job; running starts
the demonstration. There are no schedules, external libraries, or demo tables.

If you use classic compute, add `existing_cluster_id: <your-cluster-id>` to the
two top-level notebook tasks and the nested `process_country` task in
`resources/job.yml`, using a cluster on DBR 15.4 LTS or newer. Do not put compute
settings on the outer `process_countries` For each task.

## What happens

| Task | Action |
| --- | --- |
| `generate_countries` | Reads a JSON job parameter and publishes a Python country list. |
| `process_countries` | Loops over that list with concurrency 3. |
| `process_country` (nested) | Publishes one dictionary with a country and an invented row count. |
| `summarize_countries` | Reads the collected dictionaries, loops in Python, and totals the row counts. |

All three `.py` files are **Databricks Python notebooks**, deployed with
`notebook_task`. `dbutils.jobs.taskValues.set/get` are notebook utilities; these
are not standalone `spark_python_task` scripts.

The country input defaults to:

```python
["Poland", "Czechia", "Italy", "Germany", "Spain"]
```

Edit `countries_json` in the job's run parameters to try a subset or reorder it.
The demo accepts these five countries. Row counts are fixed sample numbers,
not actual processing metrics.

## The important code

Publish the input list:

```python
dbutils.jobs.taskValues.set(key="countries", value=countries)
```

Use it in the bundle, passing each item to the nested notebook:

```yaml
for_each_task:
  inputs: "{{tasks.generate_countries.values.countries}}"
  concurrency: 3
  task:
    task_key: process_country
    notebook_task:
      notebook_path: ../src/02_process_country.py
      base_parameters:
        country: "{{input}}"
```

Publish a result from each iteration:

```python
result = {"country": country, "rows_processed": demo_row_counts[country]}
dbutils.jobs.taskValues.set(key="result", value=result)
```

Read all the results in the downstream Python notebook:

```python
results = dbutils.jobs.taskValues.get(taskKey="process_country", key="result")

for result in results:
    if result is not None:
        print(result["country"], result["rows_processed"])
```

Alternatively, inject `{{tasks.process_country.values.result}}` into a notebook
parameter and parse its JSON text. The example does both and checks they agree:

```python
results_from_parameter = json.loads(dbutils.widgets.get("results_json"))
assert results == results_from_parameter
```

**Use different task keys for the dependency and the value reference:**

| Setting | Task key |
| --- | --- |
| Downstream `depends_on` | `process_countries` (outer For each) |
| `taskValues.get(taskKey=...)` | `process_country` (nested notebook) |
| Dynamic value reference | `{{tasks.process_country.values.result}}` |

## Expected output

The default run collects this list, regardless of which iteration finishes first:

```json
[
  {"country": "Poland", "rows_processed": 120},
  {"country": "Czechia", "rows_processed": 80},
  {"country": "Italy", "rows_processed": 150},
  {"country": "Germany", "rows_processed": 200},
  {"country": "Spain", "rows_processed": 90}
]
```

The Python loop prints:

```text
Poland: 120 rows
Czechia: 80 rows
Italy: 150 rows
Germany: 200 rows
Spain: 90 rows
Total rows: 640
Missing results: []
```

## Optional: show a missing value

```bash
databricks bundle run countries_demo -t dev -p DEFAULT --params skip_result_for=Italy
```

Italy's iteration succeeds but does not set `result`. Its position remains in
the output list as Python `None` / JSON `null`. The summary prints
`Italy: no result (None)`, `Total rows: 490`, and `Missing results: ['Italy']`.
The run parameter override does not change the deployed default.

This is a missing key demonstration, not a failed-task recovery mechanism.
With the default dependencies, an iteration failure prevents the summary task
from running.

## Screenshots for the article

1. The successful job graph and the five For each iterations.
2. One `process_country` iteration's `result` task value.
3. The final notebook's aggregated JSON list and `Total rows: 640` output.
4. Optionally, the second run showing Italy's `null` entry and total 490.

For cleanup, run `databricks bundle destroy -t dev -p DEFAULT` from this folder.

## Limits and references

Task values must be JSON-serializable and fit within 48 KiB. An aggregated
parameter value is limited to about 48 KB (49,344 characters), with at most
three aggregated references per parameter. Use small control values or summaries.

- [Task values and aggregated iteration outputs](https://learn.microsoft.com/en-us/azure/databricks/jobs/task-values#read-values-from-a-for-each-tasks-iterations)
- [For each tasks and downstream dependencies](https://learn.microsoft.com/en-us/azure/databricks/jobs/tasks/for-each)
- [For each configuration in bundles](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/job-task-types#for-each-task)

The Python/YAML and simulated notebook flow can be checked locally, but only a
real workspace run verifies the Databricks aggregation feature end to end.
