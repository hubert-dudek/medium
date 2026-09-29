# Primary sources

Checked **2026-09-22**. This is the feature/documentation baseline used to author the bundle, not a claim that every workspace has the same rollout state. Databricks' common SQL/UC concepts apply across clouds; links here use the AWS documentation route. Verify cloud-specific serverless availability in your own workspace.

1. **Bundle resources** — supported resource list; schemas, SQL jobs, job runs, dashboard fields and lifecycle triggers. https://docs.databricks.com/aws/en/dev-tools/bundles/resources
2. **Direct engine and warehouse lookup** — direct-engine configuration and named-object variable resolution. https://docs.databricks.com/aws/en/dev-tools/bundles/direct and https://docs.databricks.com/aws/en/dev-tools/bundles/variables
3. **Feature availability and SQL notebook tasks** — metric-feature minimums, creation prerequisites, warehouse-backed SQL-only notebook tasks. https://docs.databricks.com/aws/en/uc-semantics/metric-views/feature-availability and https://docs.databricks.com/aws/en/uc-semantics/metric-views/create and https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types
4. **Query metric views** — `MEASURE()` and aggregate-CTE-before-join pattern; current documentation does not allow general direct query-time joins. https://docs.databricks.com/aws/en/uc-semantics/metric-views/query
5. **Metric-view materialization** — YAML configuration, serverless requirements, owner/security restrictions, asynchronous refresh, rollup/exact matching and relaxed freshness. https://docs.databricks.com/aws/en/uc-semantics/metric-views/materialization
6. **System tables access** — system-catalog access and administration. https://docs.databricks.com/aws/en/admin/system-tables/
7. **Billing usage schema** — signed corrections, units and usage metadata. https://docs.databricks.com/aws/en/admin/system-tables/billing
8. **Price history schema** — effective list prices, currency and price validity periods. https://docs.databricks.com/aws/en/admin/system-tables/pricing
9. **Warehouse metadata** — change snapshots and regional coverage. https://docs.databricks.com/aws/en/admin/system-tables/warehouses
10. **Query history** — regional coverage, durations, terminal states and masked statement text. https://docs.databricks.com/aws/en/admin/system-tables/query-history
11. **Incremental refresh** — supported shapes and possible full-refresh fallback. https://docs.databricks.com/aws/en/ldp/incremental-refresh
12. **Official bundle implementations** — SQL-job metric-view example and deploy-time trigger acceptance test. https://github.com/databricks/bundle-examples/tree/main/knowledge_base/metric_view and https://github.com/databricks/cli/blob/main/acceptance/bundle/resources/job_runs/on_bundle_deploy/databricks.yml
13. **Metric YAML and advanced techniques** — join cardinality, field lineage, composable `MEASURE()` references, date offsets and cumulative windows. https://docs.databricks.com/aws/en/uc-semantics/metric-views/yaml-reference and https://docs.databricks.com/aws/en/uc-semantics/metric-views/advanced-techniques
14. **Native parameters** — `parameters`, named `=>` arguments, runtime 18.2 minimum, and no parameterized materialization. https://docs.databricks.com/aws/en/uc-semantics/metric-views/use-parameters
15. **Official dashboard JSON example** — one-chart JSON follows the dataset/query/widget shape used here. https://github.com/databricks/bundle-examples/blob/main/knowledge_base/dashboard_nyc_taxi/src/nyc_taxi_trip_analysis.lvdash.json
16. **Safe SQL identifiers and named parameters** — https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-names-identifier-clause and https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-parameter-marker

The SQL, business choices, March-calendar formulas, quality guards, example questions and local tests in this bundle are original implementation work. Sources document the APIs and product behavior; they do not certify this particular deployment.
