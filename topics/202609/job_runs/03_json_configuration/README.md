# 03 — JSON configuration into Unity Catalog Delta

Keep a small application configuration file in Git. During deployment, merge it into a Unity Catalog Delta table when the JSON or its deployment notebook changes. Applications can then read configuration from the table.

This bundle creates `main.job_runs_article.application_config`. Values are stored as strings, so the consuming application decides how to interpret values such as `30` or `0.98`. The sample notebook only stores configuration; it does not change retention, send notifications, or enforce quality rules.

## Requirements

- Databricks CLI 1.18.0 or newer, authenticated to your workspace.
- Serverless notebook jobs available in the workspace.
- Access to the target catalog and permission to create the schema/table, or the corresponding permissions on an existing schema/table.

The bundle uses the direct deployment engine. No cluster ID is required.

## Deploy and test

Run these commands inside `03_json_configuration`:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

If needed, use your own catalog/schema consistently on both commands and all later deploys:

```bash
databricks bundle deploy -t dev --var catalog=YOUR_CATALOG,schema=YOUR_SCHEMA
```

1. **First deployment:** the job runs and creates three rows in `application_config`.
2. **Deploy again without changes:** the successful tracked deployment run is reused. There should be no new job run.
3. In `config/application.json`, change `retention_days` from `"30"` to `"60"`. Add this fourth object to the array, remembering the comma after the previous object:

   ```json
   {
     "config_key": "batch_size",
     "config_value": "1000",
     "description": "Maximum records processed per application batch"
   }
   ```

4. **Deploy again:** a new run updates `retention_days` and inserts `batch_size`. The other two rows keep their existing `updated_at` values.
5. **Deploy again unchanged:** no new job run is expected.

Inspect the output grid in the notebook run, or use SQL:

```sql
SELECT config_key, config_value, description, updated_at
FROM main.job_runs_article.application_config
ORDER BY config_key;
```

After step 4, the values are:

| config_key | config_value |
|---|---|
| batch_size | 1000 |
| notification_channel | teams |
| quality_threshold | 0.98 |
| retention_days | 60 |

The exact initial row count assumes a fresh demo table. Change the query's catalog/schema if you override the defaults.

## Code to show in the article

```yaml
job_runs:
  merge_configuration:
    job_id: ${resources.jobs.merge_configuration.id}
    lifecycle:
      triggers:
        - on_file_change: ../config/application.json
        - on_file_change: ../notebooks/merge_configuration.py
```

This is an excerpt from `resources/job.yml`; the runnable version also passes the catalog/schema job parameters. The paths are relative to that YAML file.

The file watcher is evaluated by `databricks bundle deploy`; it is not a continuous background watcher. It detects content changes, including whitespace-only edits. Watching the notebook also reruns the merge if its implementation changes. Both files must remain included in bundle file synchronization.

## Screenshot suggestions

1. The JSON beside the `on_file_change` YAML.
2. First run's output grid with the three initial rows.
3. The four-row result after the edit, including `updated_at` to show which records changed.
4. Job run history plus a successful unchanged deploy showing no additional run.

## Behavior worth mentioning

- This is an **upsert**: removing a key from JSON does not delete it from Delta. The notebook intentionally has no `WHEN NOT MATCHED BY SOURCE THEN DELETE` clause.
- Duplicate keys, an empty list, invalid JSON, and non-string values fail the notebook before the merge.
- A failed deployment run can be retried by a subsequent deployment; unchanged successful runs are skipped while the resource configuration and deployment state remain unchanged.
- `databricks bundle destroy` removes bundle-managed resources. The table and schema created by notebook SQL are not bundle-managed resources and remain available.

These files have been checked locally for syntax and schema structure. They have not been deployed to a Databricks workspace; the steps above are the workspace acceptance test.
