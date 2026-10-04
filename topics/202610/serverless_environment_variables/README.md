# Serverless environment variables — simple demo

Read the [article](article.md), then use the [recording walkthrough](recording-walkthrough.md).

## Deploy the notebooks

Use an authenticated Databricks CLI profile. Replace `article-demo` below if yours has another name.

```bash
databricks bundle validate -t dev -p article-demo
databricks bundle deploy -t dev -p article-demo
databricks bundle summary -t dev -p article-demo
```

The bundle creates one job and uploads the Python notebooks and `config/application.env`. It contains **no helper scripts**. The notebooks use serverless environment version 5.

## Allow UI editing after a fresh CLI deployment

The tested CLI locks bundle-managed jobs, even when YAML requests `EDITABLE`. The prepared demo is already unlocked. To reproduce the setup, replace `123456789` with the job ID shown by `bundle summary`, then run this single command:

```bash
databricks api post /api/2.2/jobs/update -p article-demo --json '{"job_id":123456789,"new_settings":{"edit_mode":"EDITABLE"}}'
```

This enables UI editing only for your demo job. Check this again after redeployment.

## Set the variables in the UI

1. Open the deployed job and select the `simple` task.
2. Under **Environment variables**, create an entry named `app_config`.
3. Add these inline values:

| Variable | Value |
|---|---|
| `APP_ENV` | `staging` |
| `LOG_LEVEL` | `DEBUG` |
| `ARTICLE_ENV_MARKER` | `serverless-env-demo` |

4. Under **Files**, add the deployed path to `config/application.env`. Find the file in the bundle's workspace folder and copy its path; it ends in `/files/config/application.env`.
5. Save, then select the same `app_config` entry for `import_settings`, `typed_config`, and `udf_boundary`.
6. Leave **Environment variables** unassigned for `without_entry`.
7. Click **Run now**.

The current workspace job is already configured. These steps explain how to reproduce it in another workspace.

**Why this UI step?** In tested CLI v1.19.0, native environment-variable YAML fields produce warnings and disappear from resolved bundle JSON. `examples/job-environment.yml` shows the job configuration shape but is not included in the deployable bundle. After redeploying, check the entry and each task's selection again.

## Examples

| File | What it shows |
|---|---|
| `src/01_simple.py` | Required `os.environ` and optional `os.getenv` values. |
| `src/app_settings.py` | An ordinary Python file that stores shared settings at import time. |
| `src/02_import_settings.py` | Import that module and use its values in a function. |
| `src/03_typed_config.py` | Convert strings to an integer and Boolean, then validate them. |
| `src/04_udf_boundary.py` | Compare the task process with a Spark UDF. |
| `src/05_without_entry.py` | Check a task with no selected configuration entry. |

The advanced example defaults to `BATCH_SIZE=500` and `ENABLE_EXPORT=false`. You can add these two keys to `app_config` to try different values.

The demo has no schedule, writes no tables, and uses one row for the UDF. It prints only the named example values. Results are recorded in [evidence/results.md](evidence/results.md).

## Cleanup

When you finish recording, remove this demo with:

```bash
databricks bundle destroy -t dev -p article-demo
```

Review the confirmation before deletion.
