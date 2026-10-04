# Serverless environment variables

Small experiment for the [article](article.md) and [recording walkthrough](recording-walkthrough.md).

## Requirements

- A workspace with **Environment variables in Lakeflow Jobs** enabled in Previews.
- Access to serverless environment version 5 and permission to create/run a job.
- Databricks CLI on `PATH`, authenticated to your workspace. Tested with v1.19.0.
- Python 3 available as `python`. The deployment script uses only the standard library.

Use your own CLI profile in place of `article-demo`. Credentials belong in your local CLI authentication, never in this project.

## Deploy and run

From this directory:

```bash
databricks bundle validate -t dev -p article-demo
databricks bundle deploy -t dev -p article-demo
databricks bundle run -t dev -p article-demo apply_environment
databricks bundle run -t dev -p article-demo env_demo
databricks bundle summary -t dev -p article-demo
```

The bundle creates one job with three serverless notebook tasks. It has no schedule and writes no tables. The UDF test processes one row.

`apply_environment` updates the job already owned by the bundle and checks the API response. It uses the raw Jobs API because CLI v1.19.0 warns about the native bundle fields. Run it **after every deployment**: these additional fields are outside the tested bundle schema. Do not treat the two steps as an atomic production deployment.

To change the values while leaving the Python code unchanged:

```bash
databricks bundle run -t dev -p article-demo --var="app_env=staging,log_level=DEBUG" apply_environment
databricks bundle run -t dev -p article-demo env_demo
```

Here the variables are inputs to a script that explicitly updates the job. Passing `--var` to `env_demo` alone does not update its process environment. To restore the defaults, run `apply_environment` again without `--var`.

## What each task checks

| Task | Experiment |
|---|---|
| `read_environment` | Import `app_settings.py`; verify inline `APP_ENV` overrides the file and `FILE_ONLY` arrives from the file. |
| `without_entry` | Verify the demo marker and file-only setting are absent without an entry selection. |
| `udf_boundary` | Read the marker in the task process and independently inside a Spark UDF. |

Only the named synthetic values are printed. No environment dump is needed.

## Native bundle probe

```bash
cd experimental/native_dab
databricks bundle validate -t dev -p article-demo
```

This is a validation-only fixture, excluded from the working deployment. It keeps the expected native YAML shape available for retesting future CLI versions. A zero exit code with unknown-field warnings does not prove support. See [measured evidence](evidence/results.md).

## Cleanup

When you no longer need the recording job, run this from the main demo directory:

```bash
databricks bundle destroy -t dev -p article-demo
```

Review the CLI confirmation before deletion. This removes the demo's bundle-managed resources; it is not part of the recording setup.
