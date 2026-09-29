# 02 — Run on every deployment

This is a standalone bundle. Extract the ZIP into its own folder and run the
commands below from that folder, where `databricks.yml` is located.

Requirements: Databricks CLI 1.18.0 or newer, a configured authentication profile,
serverless notebook Jobs compute, and access to a UC catalog with permission to
create the demo schema/tables. The default is `main.job_runs_article`; change
`catalog` and `schema` defaults in `databricks.yml` if needed, and use the same
values throughout the test. For a named profile, add `-p YOUR_PROFILE` to commands.

Append a small deployment event to a UC Delta table. The lifecycle trigger
`on_bundle_deploy: true` requests a new run on each deployment, including an
unchanged deployment.

From this folder:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle deploy -t dev
```

Expected: both deployments run the notebook. For a clean test the job history has
two runs, and this query returns two rows:

```sql
SELECT * FROM main.job_runs_article.deployment_events
ORDER BY executed_at DESC;
```

If the table already exists from earlier testing, compare the before/after count:
these two deployments add two rows. The example uses serverless notebook compute.

This is a deployment-time run, not a callback after each scheduled/manual run of
another job. It starts when its deployment dependencies are ready; it is not a
global callback after every resource in the bundle has finished. If deployment
fails before reaching it, there is no hook execution. An audit row proves this
notebook executed, not that an entire larger bundle deployed successfully.

The CLI waits for the run; a failed run fails the deployment. Already committed
data writes are not undone. Real deployment actions should tolerate repeated
invocations.

## Screenshot for the article

Capture the two job runs or the two timestamps in `deployment_events`. Use the
`job_runs` block from `resources/job.yml` as the code excerpt. You can replace the
audit insert with your own deployment notebook logic.
