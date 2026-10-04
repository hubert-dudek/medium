# Databricks Serverless Jobs: Configure Your Python Code Before It Starts

*A small Python demo, an important Spark boundary, and a practical way to deploy it with DABs.*

Moving a Python package to serverless compute should not require rewriting how it reads configuration. If it uses `os.environ` on your laptop, keeping that interface in a scheduled job makes the package easier to reuse and test.

Databricks now offers environment variables for serverless Lakeflow Jobs. The useful part is when configuration arrives: the application can read it before importing a library.

Consider a small module that reads its settings during import:

```python
# app_settings.py
import os

APP_ENV = os.environ["APP_ENV"]
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
```

The calling code can stay simple:

```python
import app_settings

print(app_settings.APP_ENV)
```

Changing `os.environ` after that import does not update the module's stored values. Putting configuration in the process before the import avoids this ordering problem.

## What we used before

On classic compute, we could set environment variables in the compute configuration or through `spark_env_vars` in the cluster API. [Databricks documents both options](https://docs.databricks.com/aws/en/compute/configure#environment-variables).

An application-level alternative was to load a configuration file, read command-line arguments, or retrieve a value from another service, then populate `os.environ` before importing the package. That works, but adds setup code to every entry point. If someone moves an import above that setup, behavior can change.

This feature gives us another deployment option. A package can retain its ordinary Python configuration interface while the job supplies the values.

## The current configuration

The [September 28 documentation](https://docs.databricks.com/aws/en/jobs/environment-variables) describes UI and Jobs API setup. The feature remains Beta, requires an administrator to enable its preview, and needs serverless environment version 5 or later.

A job holds named entries; each task selects one with `environment_variables_key`. Entries are independent. An unassigned task receives none of these custom variables. Limits include 10 entries per job, 20 inline variables per entry, and five `.env` files per entry.

The entry uses this structure (`spec.variables` is the important nesting):

```json
{
  "environment_variables_key": "app_config",
  "spec": {
    "variables": {
      "APP_ENV": "development",
      "LOG_LEVEL": "INFO"
    },
    "files": ["/Workspace/Shared/application.env"]
  }
}
```

These variables belong to application code in the task process. Spark UDF execution is outside that scope.

## Four small experiments

The accompanying project uses synthetic values and tiny workloads. It does not need a business dataset. Each experiment asks one question:

| Experiment | What to inspect |
|---|---|
| Import a configuration module | Does it read the assigned value on its first import? |
| Run a task without an entry | Is the demo variable absent? |
| Read the same key inside a Spark UDF | Does the result differ from the task process? |
| Combine a file with inline values | Which value reaches the imported module? |

**Measured on October 4, 2026:** all three notebook tasks succeeded. The imported module read `development`, `INFO`, and the file-only value `loaded-from-file`. The unassigned task received none of the four demo variables. The task process read the marker `serverless-env-demo`; the UDF returned `null`. A second run changed only the configuration to `staging` and `DEBUG`; all tasks passed again and the same module read those new values. The [evidence folder](evidence/results.md) contains the captured results.

The first experiment matters most for application portability. The next two prevent a misleading interpretation: a job-level definition is not a global variable shared by all tasks and all Spark workers.

For a UDF, reading configuration in the calling process and deliberately passing a non-secret value into the computation is a separate design choice. It should not depend on accidental inheritance of the process environment.

## Files are useful, but read their format

Databricks reads `.env` files at task startup. Its format is literal: quotes and `${OTHER}` remain text. Later files override earlier ones; inline values win. The job's Run as identity needs file access. Use the documented format rather than assuming shell or Python dotenv behavior.

For this demo, a shared file contains defaults and the job entry contains the deployment override. That makes the recording easy to follow: show both inputs, then inspect the value actually read by the code.

## Deploying the experiment with DABs

With **Databricks CLI v1.19.0**, native bundle validation returned two warnings: `environment_variables` and `environment_variables_key` were unknown fields. Validation exited with code 0, but both fields were missing from the resolved JSON. That is why this example uses an API update. This is a result for the tested CLI version, not a claim that native support can never work.

The project keeps the job and notebooks in a bundle. After `bundle deploy`, a bundle script updates that same job through the raw Jobs API. It uses the deployed job ID rather than creating another job on every run.

Bundle substitutions pass the selected target's values to the script. The script builds the JSON entry and calls `/api/2.2/jobs/update`. The CLI's generic [`api` command](https://docs.databricks.com/aws/en/dev-tools/cli/reference/api-commands) is designed to cover API features not yet exposed by its higher-level commands.

Treat deployment and this update as one sequence, and verify the resulting job settings. Reapply the update after redeployment while using this workaround. The bundle manages the job's lifecycle; the script manages these additional settings.

I would use this pattern for deployment configuration such as application mode, logging level, or a non-secret service endpoint. Inputs that change for each run can remain [job parameters](https://docs.databricks.com/aws/en/jobs/job-parameters). The practical gain is straightforward: fewer configuration adapters around ordinary Python code.
