# Implementation and feature boundaries

## Deploy-time UC creation

There is no native metric-view resource in the current supported bundle resource list. The package uses native **schemas**, **jobs**, **job_runs** and **dashboards**, with metric DDL in SQL notebooks. The official Databricks metric-view bundle example also uses a SQL deployment job. The newer deploy-time job-run trigger removes the otherwise separate manual run step. CLI 1.13.0 is required for `lifecycle.triggers`; `job_runs` requires the direct engine. [1,2,12]

A compatible UC-enabled notebook/cluster can execute the same DDL without a SQL warehouse. This particular package uses the requested warehouse-name lookup for SQL-only job notebooks and the dashboard, so no cluster IDs/policies or serverless notebook environments are needed for deployment. [2,3]

## Screenshot concepts

| Concept | Implementation |
|---|---|
| Usage versus consumption | Canonical `billing` multiplies signed quantity by the applicable effective USD list price. |
| Custom fiscal year | All fiscal logic starts in **March**, deliberately not February from the screenshots. |
| Reusable definitions | Materialized `billing` references live `billing`; other views aggregate its measures rather than repeating the price calculation. |
| Previous-period offset | `billing_monthly` includes `offset: -1 month` and `offset: -12 month`; preflight tests actual behavior with three synthetic months. |
| Fiscal YTD windows | Cumulative month window restricted by `fiscal_year_start`; exact through-yesterday YTD uses daily billing instead. |
| Native materialization | Daily and monthly additive materializations with managed Lakeflow refresh schedules. |
| Metric-to-metric joins | Two separate `MEASURE()` aggregate CTEs, joined at an identical composite daily grain in `warehouse_efficiency`. |
| Level of detail | Fixed/coarser/finer SQL examples in `30_grain_and_lod.sql`; no unverified new LOD syntax. |
| Parameters | Separate opt-in 18.2+ temporary-view lab. No parameterized views in the deployment dependency graph. |

**The native query-time JOIN screenshot is not treated as generally available.** Current public query documentation explicitly requires aggregating a metric view in a CTE before joining it. That supported pattern is what is deployed. No unsupported direct metric-to-metric join is silently included as production SQL. [4]

Dates/months for the window hierarchy reference earlier metric fields, rather than repeating the underlying source column expression. Reporting filters are applied outside the offset result in examples. This keeps earlier comparison periods available. Missing periods remain NULL: the bundle does not invent zero-valued historical months. [13]

## Runtime/engine support to check

| Capability used | Documented minimum |
|---|---|
| Core metric views | 16.4 |
| Materialization, temporary metric views and richer metadata | 17.3 |
| Explicit `REFRESH MATERIALIZED VIEW` for a metric view | 18.0 |
| Dated offsets used in the default bundle | 18.1 |
| Native parameter lab | 18.2 |

These are runtime capability levels, not a `spark_version` to set on a SQL warehouse. Warehouse channels and workspace availability must actually support the features. No fictitious runtime configuration is applied to the existing warehouse. Numeric index offsets, which have a different minimum, are not required here. [3]

## Materialization verification

The managed pipeline is created by the metric-view definition; no DAB `pipelines` resource should be added for the same work. Inspect `DESCRIBE TABLE EXTENDED`, the managed pipeline update, and the query profile/plan. The materialization notebook looks for documented internal materialization scan markers as a diagnostic, not as a substitute for inspecting the plan. Window queries are not guaranteed to match the additive rollups. [5]

`relaxed` mode permits stale materializations. This package deliberately avoids equality assertions between live and materialized totals unless an operator first aligns refreshes and source state. Both monthly definitions read the **live** canonical billing measure, not each other's refreshed snapshots; independent refreshes therefore do not create an avoidable stale-on-stale chain.

## Fiscal and data-coverage caveats

The calendar formulas and reference tests are specific to a regular March–February year, not a 4–4–5 retail calendar. `ADD_MONTHS` governs the leap-date alignment in same-period comparisons. On March 1, current FYTD excluding today is an empty interval.

Source dates are observations, not coverage guarantees. A warehouse with no query records can be idle, outside the current region, or missing history. Matching one observed query does not prove all statements arrived. Guarding unmatched days is conservative but cannot prove complete telemetry. An invoice reconciliation or exact per-query chargeback needs additional contract, credit and allocation data not present in this bundle.

References: [sources.md](sources.md).
