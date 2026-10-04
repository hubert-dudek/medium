# Measured results — 4 October 2026

The simplified demo uses CLI **v1.19.0** and serverless environment **5**. Run **497355485047436** finished with **SUCCESS**. All five notebook tasks succeeded and returned the exact expected values.

## Deployment and configuration

- The bundle deploys the job, five notebook examples, `app_settings.py` and the `.env` file. There are no deployment helper scripts.
- The job was deployed through the CLI. A raw Jobs API update then made the existing demo job editable with `edit_mode: EDITABLE` and applied the same values shown in the screenshots: inline `APP_ENV=staging`, `LOG_LEVEL=DEBUG` and `ARTICLE_ENV_MARKER=serverless-env-demo`, plus the `.env` file.
- The configured entry was assigned to `simple`, `import_settings`, `typed_config` and `udf_boundary`. `without_entry` had no entry assignment.
- The API update prepared this recording workspace. It is not a script that readers need to maintain. The [README](../README.md) gives the UI steps for creating the entry and assigning it after deployment.
- The file supplied `APP_ENV=from-file`, `LOG_LEVEL=WARNING` and `FILE_ONLY=loaded-from-file`. The outputs below confirm that the inline values won and the file-only value remained available.

## Native DAB fields: tested CLI limitation

| Check | Observed result |
|---|---|
| Native bundle validation | Exit code 0, with unknown-field warnings for `environment_variables` and task `environment_variables_key`. |
| Resolved native bundle JSON | Both native fields were absent. |

The native probe was not deployed. Its warning log is retained in [native-validation.txt](native-validation.txt). These observations apply to CLI **v1.19.0**; a successful validation exit code alone did not demonstrate that the fields were retained.

For this version, use DABs for the notebooks and job, then apply the entry through the UI. After another bundle deployment, check the entry and all task assignments again before running.

## Verified runtime outputs

Notebook exit results were retrieved with the CLI. Every output was untruncated, parsed as JSON and compared with its exact expected result.

| Task | Status | Verified output |
|---|---|---|
| `simple` | SUCCESS | `APP_ENV=staging`, `LOG_LEVEL=DEBUG`, `FILE_ONLY=loaded-from-file` |
| `import_settings` | SUCCESS | `APP_ENV=staging`, `LOG_LEVEL=DEBUG`, `FILE_ONLY=loaded-from-file` |
| `typed_config` | SUCCESS | `app_env=staging`, `batch_size=500` as an integer, `enable_export=false` as a boolean |
| `udf_boundary` | SUCCESS | `task_process=serverless-env-demo`, `spark_udf=null` |
| `without_entry` | SUCCESS | `APP_ENV`, `LOG_LEVEL`, `FILE_ONLY` and `ARTICLE_ENV_MARKER` all `null` |

The typed example uses its documented defaults for the integer and boolean settings; those values were not added to the UI entry.

See [simplified-run.json](simplified-run.json) for the run ID, task IDs, statuses and parsed outputs. Workspace identity and host details are omitted.

## Scope

These results cover this CLI release, environment version and five-notebook demo. They do not establish behavior for secrets, every task type, all file-format edge cases or future CLI releases. The demo uses non-secret settings. CLI deployment and subsequent UI configuration are separate steps.
