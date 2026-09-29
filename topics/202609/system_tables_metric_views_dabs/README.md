# Databricks system-table metric views

Two exact Unity Catalog schemas in **`main`**, a **March–February fiscal year**, four live metric views, two billing metric views with **native metric-view materialization**, and one AI/BI dashboard.

## Deploy

Use a configured Databricks CLI **1.13.0 or newer** and an existing **serverless or Pro** SQL warehouse named exactly **`SQL warehouse`**. Warehouse feature support must be equivalent to Runtime **18.1+** for the default offset examples. Classic SQL warehouses cannot run the deployment notebook tasks. The optional parameter lab requires **18.2+**. [1–4]

```bash
cd system_tables_metric_views_dabs

databricks bundle validate -t main
databricks bundle deploy -t main
```

Authentication uses your normal Databricks CLI profile/environment. For a named profile, add `--profile YOUR_PROFILE` to both commands. No credentials or workspace URL are embedded.

`bundle deploy` creates the schemas, job and dashboard and triggers the SQL deployment job through **`resources.job_runs`**. This uses the **direct deployment engine** and `on_bundle_deploy`, not an invented native `metric_views` resource. Follow the deployment job to **SUCCESS** before querying the views. To reapply the SQL explicitly:

```bash
databricks bundle run deploy_metric_views -t main
databricks bundle summary -t main
```

**Do not run the explicit job command immediately after deploy unless you intend a second run.** The first managed materialization refresh is asynchronous; SQL creation success does not mean its physical results are ready. A query can use source fallback. [1,5]

The default target deliberately has no development-mode name prefix. It writes the exact schema names requested. Do not deploy several independent bundle states to these same schemas. Existing schemas or pre-existing owned metric views require intentional ownership/state adoption before first deployment; this bundle assumes its two schema names are available.

## What is created

| Schema | Object | Purpose |
|---|---|---|
| `main.system_tables_metrics` | `warehouse_latest` | Ordinary helper view: one latest metadata row per account/workspace/warehouse. |
| same | `billing` | Canonical signed usage × time-valid USD price; fiscal dimensions, resource IDs and tags. |
| same | `billing_monthly` | Reuses billing measures; `-1 month`, `-12 month`, growth and March fiscal-YTD windows. |
| same | `warehouse_queries` | Regional SQL-warehouse query counts, failures and durations. |
| same | `warehouse_efficiency` | Joins **aggregated results of two metric views**, not raw billing rows to query events. |
| `main.system_tables_metrics_materialized` | `billing` | Explicit semantic mirror of live billing, with a daily-grain managed materialization. |
| same | `billing_monthly` | Same monthly semantics, with a monthly-grain managed materialization. |

Both materialized definitions specify `materialization:`, `mode: relaxed` and an **every 6 hours** schedule. Databricks owns the supporting Lakeflow pipelines; there is no second hand-written DAB pipeline. The additive materializations support rollups. The monthly definition also exposes window measures, but this bundle does **not** promise that offset/YTD queries will be accelerated: those can use source fallback. [5]

The dashboard JSON is `dashboards/billing_fiscal_ytd.lvdash.json`: **one dataset and one bar chart** over `main.system_tables_metrics_materialized.billing`. It groups estimated USD consumption by month in the current March fiscal year, through yesterday UTC. Viewer credentials are used; the bundle does not grant broad access or embed the deployment identity's credentials. [1]

## Prerequisites and privileges

The deployment identity needs `USE CATALOG` and `CREATE SCHEMA` on the existing catalog `main`; ownership or appropriate `USE SCHEMA` / `CREATE TABLE` privileges in the target schemas; and `USE CATALOG`, `USE SCHEMA`, and `SELECT` access to the four system tables below. It also needs warehouse `CAN USE` and workspace permission to create the job/dashboard. The same identity must remain able to read all source dependencies. [3,6]

Sources, all already provided by Databricks:

```
system.billing.usage
system.billing.list_prices
system.compute.warehouses
system.query.history
```

The relevant system schemas must be enabled and visible in this workspace/metastore. Query history has stronger default access restrictions than ordinary user data. The bundle does not enable system schemas, modify source tables, or grant account-wide permissions. [7–10]

Materialization additionally needs **serverless Lakeflow availability** and the privileges/storage setup required for managed materializations. Source RLS, column masks or ABAC can block materialization; this is not bypassed. Query-history metrics are deliberately live-only. Do not change a materialized metric view's owner casually; ownership changes have restrictions. [5]

## Business definitions

**Spend is an estimate, not invoice reconciliation.** `spend_usd` uses `pricing.effective_list.default` in USD and includes the signed ORIGINAL/RETRACTION/RESTATEMENT records. It does not model negotiated prices, account credits, taxes, commitments or separately billed cloud infrastructure. The price join includes account, SKU, cloud, usage unit and the complete usage time interval. A record spanning a price boundary is unpriced rather than arbitrarily prorated. [7,8]

`priced_cost_usd` is only a subtotal. `unpriced_record_count > 0` makes `spend_usd` NULL, rather than quietly treating unknown prices as zero. DBUs are a separate measure; quantities with unlike units must not be summed. The deployment preflight rejects overlapping USD price intervals.

`fiscal_year` is the **start year**: `FY2026/27` runs 2026-03-01 through 2027-02-28. Q1 = Mar–May, Q2 = Jun–Aug, Q3 = Sep–Nov, Q4 = Dec–Feb. Exact FYTD examples use the half-open UTC interval `[March 1, cutoff_date)`. The monthly YTD measure is a snapshot and can include partial current-month data; it must not be added across months.

Billing is account-wide, whereas query history and warehouse metadata are regional. A latest warehouse name is not a historical as-of name. `warehouse_efficiency` preserves unmatched warehouse-days and suppresses its blended ratio when coverage is missing. Its cost per observed statement is **not** precise per-query cost attribution. Late arrivals and retained-history limits can make prior-period comparisons incomplete. [7,9,10]

## Examples

Open the synced notebooks under the bundle's workspace files directory:

- `notebooks/10_engineering_examples.py`: safe `spark.sql(..., args=...)`, explicit `IDENTIFIER()`, FYTD, SKU/quarter, offsets, same-period previous FY, missing prices and downstream DataFrames.
- `notebooks/20_materialization.py`: metadata, optional manual refresh and `EXPLAIN EXTENDED` inspection. Read-only unless `refresh_now=true`.
- `notebooks/30_grain_and_lod.sql`: fixed, coarser and finer analytical grains using standard SQL.
- `notebooks/90_native_parameters_lab.py`: separate **opt-in** 18.2+ lab, with a hypothetical discount and a parameterized month offset. It creates only a temporary metric view and never materializes it.

`docs/genie.md` contains ready-to-paste instructions and example questions/SQL. `docs/feature_notes.md` explains which screenshot concepts are deployed and which are not assumed to exist in the target workspace.

## Safety, operations and testing

The SQL runs `CREATE OR REPLACE VIEW`; it does not ingest copies of billing/query-history tables or create a business-data archive. Native schemas have `prevent_destroy: true`. UC views created by the job are **not individual bundle state resources**. Renaming/removing a SQL file does not automatically drop the old UC view. There is no cross-object transaction or automatic rollback for a partly failed deployment; fix the failed task and rerun the job. Do not expect `bundle destroy` to remove protected schemas or stop the materializations they contain.

**Materialization schedules incur compute/storage costs after deployment**, even when the deployment job is idle. Incremental refresh is best effort and can fall back to full recomputation for these source/query shapes. Before production use, inspect pipeline costs, source freshness and your query profiles. Remove/alter the metric view's materialization deliberately to stop this scheduled work. [5,11]

Offline tests:

```bash
python -m pip install -r requirements-dev.txt
python -m unittest discover -s tests -v
```

See `VALIDATION.md` for exactly what was tested. This package has not been deployed to your workspace and does not claim successful live SQL execution, refreshes or dashboard import. The deployed preflight and smoke tasks provide the next layer of validation.

## Sources

Numbered references resolve to official documentation and Databricks repositories in [docs/sources.md](docs/sources.md). Research checked **2026-09-22**.
