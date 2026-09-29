# 04 — Validation fails; production stays on A

This variant deliberately fails candidate validation. Both jobs use the same short notebook. The only code difference between the two variants is `MULTIPLIER`: `-1` fails, `2` succeeds. There is no JSON, widget, parameter, table, or persistent data write.

**Private Preview prerequisite:** ask Databricks to enable `experimental.immutable_folder` in your workspace. Workspace files and serverless notebook jobs must be available. Use CLI 1.18.0 or newer with workspace authentication. The direct engine and `source_linked_deployment: false` are already configured. If snapshots are unavailable, stop; removing immutability invalidates the previous-code preservation test.

## One deployment, two variants

These two folders deliberately use the **same bundle name, job keys, target, and deployment state**. Each is a complete DAB; deploy them in sequence to the same workspace using the same authenticated identity/profile. Keep `bundle.name`, target `dev`, and `workspace.root_path` unchanged. Do not deploy them concurrently. They are variants of one deployment, not isolated deployments.

Extract both folders next to one another:

- `04_validation_fails`
- `04_validation_succeeds`

## 1. Seed release A first

Start inside `04_validation_succeeds`. Temporarily edit the first code cell in `notebooks/candidate.py`:

```python
RELEASE = "A"
MULTIPLIER = 1
```

From that folder, run:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run production_job -t dev
databricks bundle summary -t dev
```

Expected: successful deployment and `VALIDATION PASSED: release A; adjusted amounts [10, 20, 30]`. In the production job UI, record its ID and its task's snapshot notebook path. The tag shows `validation_result = SUCCESS`.

Now restore the success notebook to its supplied values, **without deploying it yet**:

```python
RELEASE = "B"
MULTIPLIER = 2
```

Screenshot: **A's successful output and production job's snapshot path.**

## 2. Deploy the failing B variant

From inside `04_validation_succeeds`, switch to its sibling:

```bash
cd ../04_validation_fails
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

The supplied notebook has `RELEASE = "B"` and `MULTIPLIER = -1`. Expected: a run error containing `VALIDATION FAILED: release B; adjusted amounts [-10, -20, -30] must all be positive`, and a failed deploy command.

Check the **same production job ID** recorded after A:

```bash
databricks jobs get <production-job-id> -o json
databricks bundle run production_job -t dev
```

Expected: its task's notebook path still points to snapshot A, and running it still prints `VALIDATION PASSED: release A; adjusted amounts [10, 20, 30]`. A `SUCCESS` tag alone is insufficient proof: compare the actual notebook path and the release printed by the run.

Screenshots: **B's failed validation/deploy; then the same production job still running A.**

## 3. Deploy the successful B variant

From inside `04_validation_fails`, switch back:

```bash
cd ../04_validation_succeeds
databricks bundle deploy -t dev
databricks bundle run production_job -t dev
databricks jobs get <production-job-id> -o json
```

Expected: B passes validation, the same production job advances to B's snapshot, and its run prints `VALIDATION PASSED: release B; adjusted amounts [20, 40, 60]`.

Screenshot: **B's successful deployment, new production snapshot path, and doubled amounts.**

The accompanying infographic labels the fixed candidate snapshot **B′**. The notebook still calls the release B, but fixing its code creates a different snapshot from the failed B candidate.

## Why it works, and what it does not promise

The production job tag references `${resources.job_runs.validate_candidate.state.result_state}`. That reference creates a deployment dependency on successful candidate validation. The separate validation job prevents a dependency cycle. Immutable snapshots preserve A's files while B is tested.

This is a validation gate, not whole-bundle rollback. Independent resources can change, and the validation job itself is updated before the failure. No data changes are reversed. On fresh state, deploying the failing variant first demonstrates blocked production-job creation only; seed A to demonstrate preservation of an existing release. Enable immutability from the first A deployment. Administrators can still modify snapshot files.

Local checks cover the CLI schema, Python syntax, paths, matching deployment identities, and expected fixture values. **The combined workspace behavior has not been runtime-tested here; these steps are the test.**

## Cleanup and sources

From either folder, `databricks bundle destroy -t dev` removes the same shared demo jobs. It does not promise to delete historical runs or retained snapshots.

- [Bundle reference: immutable folders](https://docs.databricks.com/aws/en/dev-tools/bundles/reference#experimental)
- [CLI v1.18.0: failed run blocks a dependent job](https://github.com/databricks/cli/blob/v1.18.0/acceptance/bundle/resources/job_runs/failed_run/databricks.yml.tmpl)
- [CLI v1.18.0: immutable snapshot implementation](https://github.com/databricks/cli/blob/v1.18.0/bundle/direct/dresources/snapshot.go)
