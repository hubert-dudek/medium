# Changes and sources — checked 4 October 2026

Primary draft source: the user-provided September 2 conversation about serverless environment variables.

## Corrections incorporated

- Replace the old API-only description with UI and API setup.
- Replace the old 100 inline variables claim with 20.
- Correct entry structure to `spec.variables` and `spec.files`.
- Add file precedence and literal-format details briefly.
- Keep Beta, preview enablement, version 5+, task assignment, and the Spark boundary.
- Keep native DAB support claims tied to the exact installed CLI schema and validation output.
- Update an existing DAB-managed job instead of creating unmanaged duplicate jobs.
- Remove configuration adapters involving widgets and remove speculative contributor quotations.
- Replace predictions with measured notebook outputs and CLI v1.19.0 validation evidence.

## Official references

1. [Environment variables for serverless jobs](https://docs.databricks.com/aws/en/jobs/environment-variables), updated September 28, 2026. Verified all current limits and syntax.
2. [Compute configuration](https://docs.databricks.com/aws/en/compute/configure#environment-variables): classic configuration and `spark_env_vars`.
3. [CLI API commands](https://docs.databricks.com/aws/en/dev-tools/cli/reference/api-commands): raw commands for capabilities not covered by higher-level CLI groups.
4. [Bundle configuration](https://docs.databricks.com/aws/en/dev-tools/bundles/settings): bundle resource lifecycle and terminology (Declarative Automation Bundles; formerly Databricks Asset Bundles).
5. [Bundle variables](https://docs.databricks.com/aws/en/dev-tools/bundles/variables): deployment substitutions.
6. [Jobs API update](https://docs.databricks.com/api/workspace/jobs/update): update endpoint. The API reference now redirects to `/api/jobs/v2/update-job` but lists `POST /api/2.2/jobs/update`.
7. [Job parameters](https://docs.databricks.com/aws/en/jobs/job-parameters): run inputs and overrides.
8. [AI Runtime bundle tasks](https://docs.databricks.com/aws/en/machine-learning/ai-runtime/configure-bundle-tasks): explicitly mentions environment-variable job/task fields. Therefore do not say “no bundle documentation mentions these fields”; test actual CLI behavior.

## Measured evidence

See [results.md](results.md), the raw warning excerpt, and the JSON task outputs in this directory. The native probe was not deployed.
