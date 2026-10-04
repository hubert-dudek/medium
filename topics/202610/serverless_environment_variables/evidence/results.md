# Measured results — 4 October 2026

Tested in the connected AWS Databricks workspace with CLI **v1.19.0** and serverless environment **5**. Only the new demo job was modified. The job remains available for recording with **staging / DEBUG** selected; both successful runs are in its history.

## Configuration and deployment

| Check | Observed result |
|---|---|
| Native DAB probe | Exit code 0, two unknown-field warnings. |
| Resolved native bundle JSON | Both `environment_variables` and task `environment_variables_key` absent. |
| Working bundle validation | `Validation OK!` |
| Working bundle deployment | One job created; eight files uploaded. |
| Default API application | Raw GET confirmed the exact entry and both selected tasks. |
| Second API application | Updated the same job to staging/DEBUG; raw GET matched. No duplicate job created. |

The native probe was **not deployed**. Its warning log is in [native-validation.txt](native-validation.txt). Current API documentation alone does not prove that a particular CLI release retains its fields.

## Runtime checks

Default run: **987665494149478**, all three tasks **SUCCESS**. Notebook exit results were retrieved through the CLI, checked for truncation, parsed, and compared against the exact expected values.

| Task / assertion | Observed value |
|---|---|
| Import-time `APP_ENV` | `development` |
| Import-time `LOG_LEVEL` | `INFO` (file default was `WARNING`) |
| File-only setting | `loaded-from-file` |
| Unassigned task | APP_ENV, LOG_LEVEL, FILE_ONLY and ARTICLE_ENV_MARKER all `null` |
| Marker in selected task process | `serverless-env-demo` |
| Same marker read independently inside Spark UDF | `null` |

See [default-run.json](default-run.json).

Changed-configuration run: **1081695220293683**, all three tasks **SUCCESS**. Without editing the Python code, the selected module returned `staging`, `DEBUG`, and `loaded-from-file`. These values were also compared exactly. See [staging-run.json](staging-run.json).

## Scope

These observations cover this CLI version, this workspace and this environment version. They do not establish support for secrets, every serverless task type, all `.env` format rules, or a future CLI. The sample uses non-secret values. Bundle redeployment after the API update was not tested; the documented workflow always reapplies the API fields after deployment. The two deployment steps are not atomic.
