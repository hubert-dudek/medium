# Recording walkthrough — about four minutes

Suggested title: **Databricks Serverless Environment Variables: Python Demo + DABs**

The main story is configuration available when a module is first imported. Keep the UI, notebook output, and deployment commands visible enough to read. The Spark experiment is the boundary check, not the opening explanation.

## Before recording

1. Review the measured outputs in `evidence/results.md`.
2. Open the job, its successful run, `src/app_settings.py`, and `databricks.yml` in separate tabs.
3. Use the experiment's named synthetic variables. Show only those outputs.
4. Save a clean terminal command history containing the commands below. Do not display authentication setup.
5. Use the latest matching run for the values you show: development/INFO or staging/DEBUG.

```bash
databricks bundle validate -t dev -p article-demo
databricks bundle deploy -t dev -p article-demo
databricks bundle run -t dev -p article-demo apply_environment
databricks bundle run -t dev -p article-demo env_demo
```

## Shot list and narration

| Time | Screen action | Suggested narration |
|---|---|---|
| 0:00–0:25 | Show the configuration module and its first import. | “This Python module reads configuration when it is imported. I want the same code to work locally and in a Databricks serverless job. The job should supply the values before the application reads them.” |
| 0:25–0:55 | Open the selected task and its environment-variable entry. Highlight the entry key and application mode. | “This is a named configuration entry attached to a specific task. The application reads it through Python's ordinary process environment. There is no configuration adapter in the module.” |
| 0:55–1:25 | Open the first task's recorded output beside the import. | “The first experiment checks the value captured during import. Read this output alongside the job configuration. The output shows the received values; the assertions check file loading and precedence.” Show the development/INFO output or the later staging/DEBUG output. |
| 1:25–1:50 | Show the unassigned task's configuration and output. | “I also created a task without that assignment. This is a control experiment. It tells us whether the job-level entry was automatically applied more widely than intended.” |
| 1:50–2:25 | Show the UDF definition and a one-row result. | “Now the code reads the same key inside a Spark UDF. Compare that result with the application process. These are separate execution contexts, so the recording needs to show both.” The measured values are `serverless-env-demo` in the task and `null` in the UDF. |
| 2:25–2:55 | Show the demo `.env` file, the inline override, then output. | “The file provides a default. This inline value represents our deployment choice. The final experiment checks which value the application actually received.” `FILE_ONLY=loaded-from-file` proves the file was read; `LOG_LEVEL` proves the inline override. |
| 2:55–3:40 | Show the bundle variable, the script's `env` mapping, and the three deployment steps. | “The bundle deploys the job and notebooks. With the CLI version tested here, a separate script applies the environment settings to that existing job through the Jobs API. Bundle substitutions provide the selected values.” CLI v1.19.0 warns about both native fields and drops them from resolved JSON; show `evidence/native-validation.txt`. |
| 3:40–4:00 | Return to the simple configuration module and actual successful output. | “The benefit is keeping application configuration outside the Python package. That makes a migration easier to reason about. The task assignment and Spark boundary are the two details to check before adopting it.” |

For the optional configuration-change shot, run:

```bash
databricks bundle run -t dev -p article-demo --var="app_env=staging,log_level=DEBUG" apply_environment
databricks bundle run -t dev -p article-demo env_demo
```

The script explicitly updates the deployed job; `--var` on the job run alone would not inject these settings.

## Optional contributor questions

1. Which application migration problem most influenced the design of environment variables for serverless jobs?
2. What should developers understand about the boundary between a task process and Spark execution when moving existing code to serverless?
3. What is the recommended deployment path while a workspace API capability and a team's installed bundle CLI expose different configuration fields?

Use a response only after the contributor approves the wording and attribution. There is no quotation placeholder to publish as if it were a real statement.
