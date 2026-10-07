# Run jobs and pipelines as a group

Databricks CLI 1.20.0 adds `run_as.group_name` at the bundle and target levels. The setting is inherited by jobs and pipelines; an explicit resource-level identity is preserved.

```yaml
run_as:
  group_name: medium-run-as-demo
```

```yaml
targets:
  target_level:
    run_as:
      group_name: medium-run-as-demo
```

The example uses the direct deployment engine and serverless compute:

- `group_job` runs `SELECT current_user() AS run_as, 42 AS answer;`.
- `group_pipeline` creates a one-row materialized view with `answer = 42`.
- `user_override` keeps the deploying user's identity explicitly.
- `bundle_level` inherits the top-level group; `target_level` also declares the group in its target.

## Verified on 2026-10-07

Both targets passed bundle validation with CLI 1.20.0. The resolved job and pipeline run-as identities were the group, while `user_override` remained the deploying user.

The `bundle_level` job and pipeline were deployed and run successfully in a serverless workspace. The job returned:

| run_as | answer |
| --- | --- |
| medium-run-as-demo | 42 |

The pipeline completed and created `workspace.medium_run_as_group_bundle_level.group_result`, owned by `medium-run-as-demo`. The alternate target and explicit user override were validated but not run.

## Setup and run

Use an account group assigned USER access to the workspace. The demo group contains only the deploying user. Group execution uses the group's privileges, not the member's personal privileges.

The bundle grants the group CAN_VIEW on its own resources and read access to its deployed files. For the pipeline, create the isolated schema and grant:

```sql
CREATE SCHEMA IF NOT EXISTS workspace.medium_run_as_group_bundle_level;
GRANT USE CATALOG ON CATALOG workspace TO `medium-run-as-demo`;
GRANT USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW
ON SCHEMA workspace.medium_run_as_group_bundle_level TO `medium-run-as-demo`;
```

```bash
databricks bundle validate -t bundle_level
databricks bundle validate -t target_level
databricks bundle deploy -t bundle_level
databricks bundle run group_job -t bundle_level
databricks bundle run group_pipeline -t bundle_level
```

No schedule is configured. Deploying `target_level` creates a separate set of resources and requires its own schema and scoped grants.

Sources: [CLI change #6676](https://github.com/databricks/cli/pull/6676), [CLI 1.20.0](https://github.com/databricks/cli/releases/tag/v1.20.0), [job identities and privileges](https://docs.databricks.com/aws/en/jobs/privileges), [bundle permissions](https://docs.databricks.com/aws/en/dev-tools/bundles/permissions).
