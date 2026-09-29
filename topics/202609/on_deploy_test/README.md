# Minimal on-deploy job test

One serverless notebook job and one `job_runs` resource using your resource keys.
The notebook prints a success message and a UTC timestamp. It creates no tables
or metric views.

Requirements: Databricks CLI 1.13.0 or newer, an authenticated workspace profile,
and permission to create jobs and use serverless jobs compute. The bundle explicitly
uses the direct deployment engine. No notebook environment or dependencies are needed.

From the extracted `on_deploy_test` folder, replace `YOUR_PROFILE` with your CLI
profile name and run:

```bash
databricks bundle validate -t dev -p YOUR_PROFILE
databricks bundle deploy -t dev -p YOUR_PROFILE
databricks bundle deploy -t dev -p YOUR_PROFILE
```

Wait for the first deploy to finish before running the second. Each deploy should
start a new job run, including when no files change. Deployment waits for the run
to finish; a failed run causes deployment to fail. No separate `bundle run` is needed.

Open Jobs & Pipelines, find the development-prefixed `on_deploy_test` job, and
check its two successful runs. The notebook output should show a different UTC
timestamp for each run.

The `job_runs` mapping belongs under `resources`, alongside `jobs`.

Optional cleanup:

```bash
databricks bundle destroy -t dev -p YOUR_PROFILE
```

The YAML and Python syntax were checked locally. This example has not been deployed
to a Databricks workspace from this session.

References:

- [Job run resource and lifecycle requirements](https://docs.databricks.com/aws/en/dev-tools/bundles/resources#job_run)
- [CLI release notes: 1.13.0 adds the deployment trigger; 1.12.0 adds run waiting](https://github.com/databricks/cli/releases)
- [Direct deployment engine](https://docs.databricks.com/aws/en/dev-tools/bundles/direct)
