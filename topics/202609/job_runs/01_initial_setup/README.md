# 01 — Initial setup

This is a standalone bundle. Extract the ZIP into its own folder and run the
commands below from that folder, where `databricks.yml` is located.

Requirements: Databricks CLI 1.18.0 or newer, a configured authentication profile,
serverless notebook Jobs compute, and access to a UC catalog with permission to
create the demo schema/tables. The default is `main.job_runs_article`; change
`catalog` and `schema` defaults in `databricks.yml` if needed, and use the same
values throughout the test. For a named profile, add `-p YOUR_PROFILE` to commands.

Create initial application settings on the first deployment. The `job_runs`
resource has no lifecycle triggers. An unchanged deployment reuses the previous
successful run.

From this folder, with Databricks CLI 1.18.0 or newer and a configured profile:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Open the `initialize` job's run history. The notebook creates
`main.job_runs_article.bootstrap_settings` and appends one row to
`main.job_runs_article.bootstrap_calls`.

Deploy again without changing anything:

```bash
databricks bundle deploy -t dev
```

Expected: no new initialization run and no additional audit row. Use SQL to
compare the count before and after your second deployment:

```sql
SELECT * FROM main.job_runs_article.bootstrap_settings;
SELECT count(*) AS initialization_calls
FROM main.job_runs_article.bootstrap_calls;
```

The count is 1 only for a clean first test. Earlier demo executions can leave
audit rows in this schema.

## What “once” means

This is initial creation followed by reuse of a successful, unchanged run in the
same deployment state. It is not an exactly-once guarantee. Changing the run's
parameters or job ID, a failed/missing historical run, or deploying with fresh
state can execute setup again. A notebook-only edit does not itself retrigger
this run. The setup uses `IF NOT EXISTS` so repeated execution preserves existing
settings; the audit append deliberately records every invocation.

Do not add `prevent_destroy: true` as a run-once switch: it can block a required
replacement instead of silently skipping execution.

## Screenshot for the article

Capture the second successful deployment beside the job history with just one
initialization run, or the unchanged audit count. Use the `job_runs` block from
`resources/job.yml` as the code excerpt.
