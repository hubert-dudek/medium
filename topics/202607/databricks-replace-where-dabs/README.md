# Six REPLACE WHERE use cases as a Databricks bundle

This project is a deployable **Declarative Automation Bundle** (DAB; formerly Databricks Asset Bundle) containing:

- one shared Unity Catalog source schema with deterministic sample data;
- one output schema;
- **six independent Lakeflow pipelines**;
- two Materialized View REPLACE examples and four Streaming Table REPLACE examples;
- one initialization workflow;
- one mutation-and-refresh demo workflow for every use case;
- a request monitor that launches parameterized historical backfills through the Pipelines API;
- validation queries and a standalone example API payload.

The sample values, including VAT rates and weather data, are synthetic and intended only to demonstrate replacement behavior.

## The six use cases

| # | Target | Core replace boundary | Demonstration |
|---|---|---|---|
| 1 | `sales_by_product_spec` (MV) | `order_date >= current_date() - recompute_days` | A nested `VARIANT` product specification changes, but only a parameterized recent horizon adopts it. |
| 2 | `invoice_vat_calculation` (MV) | `invoice_date >= vat_recompute_from` | A VAT rule changes from 1 January; invoices before that legal boundary remain frozen. |
| 3 | `weather_forecast_current` (ST) | normally `forecast_date >= current_date()` | Every ordinary update replaces today and future forecasts with the newest issued snapshot. |
| 4 | `customer_daily_metrics` (ST) | `metric_date BETWEEN backfill_start_date AND backfill_end_date` | A request-table monitor passes range parameters into a one-time pipeline update. |
| 5 | `invoice_reporting_open_period` (ST) | entity plus `accounting_date >= open_period_start` | Open periods recalculate after facts, FX, or JSON allocations change; closed periods do not. |
| 6 | `settlement_canonical` (ST) | provider plus rolling restatement horizon | A reissued authoritative file replaces a whole day; rows missing from the new version are removed. |

## Project layout

```text
.
├── databricks.yml
├── resources/
│   ├── schemas.yml
│   ├── pipelines.yml
│   └── jobs.yml
├── src/
│   ├── setup/00_create_sample_data.sql
│   ├── pipelines/01_...sql through 06_...sql
│   ├── orchestration/04_backfill_monitor.py
│   ├── mutations/01_...sql through 06_...sql
│   └── validation/00_validate_outputs.sql
├── examples/
│   ├── scenario_queries.sql
│   └── start_update_with_parameters.json
└── tests/static_checks.py
```

## Prerequisites

- A Unity Catalog-enabled Databricks workspace.
- Permission to create schemas in the selected catalog.
- Serverless Lakeflow pipelines and Serverless Jobs enabled.
- Pipeline parameters enabled in the workspace Previews page.
- Databricks CLI `0.255.0` or newer. The bundle enforces this because it uses `experimental.skip_name_prefix_for_schema` so development-mode schema names remain deterministic.
- The Materialized View REPLACE build/syntax supplied for the first two examples. Those two SQL files intentionally use:

```sql
CREATE OR REFRESH MATERIALIZED VIEW ...
FLOW REPLACE WHERE ... BY NAME
SELECT ...
```

The remaining four examples use Streaming Table REPLACE syntax.

## Configure

The default development deployment uses:

```text
main.replace_where_demo_src_dev
main.replace_where_demo_out_dev
```

Override the catalog during validation, deployment, and runs when required:

```bash
databricks bundle validate -t dev --var="catalog=my_catalog"
databricks bundle deploy -t dev --var="catalog=my_catalog"
```

Schema names are generated from `source_schema_base`, `output_schema_base`, and the bundle target. Change those variables in `databricks.yml` or pass `--var` overrides. The bundle has one `dev` target to keep the example compact.

## Deploy and initialize

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev initialize_all_examples
```

`initialize_all_examples` recreates the source data and starts all six pipelines. It temporarily pushes broad parameter values into the pipeline tasks so the complete sample history is seeded:

- `recompute_days=3650`;
- `vat_recompute_from=1900-01-01`;
- `weather_history_days=3650`;
- a full backfill date range;
- `open_period_start=1900-01-01`;
- `restatement_lookback_days=3650`.

Those values apply only to the initialization run. The saved pipeline defaults remain narrow and scenario-specific.

Run the validation notebook after initialization:

```bash
databricks bundle run -t dev validate_outputs
```

## Run each demonstration

Every command first mutates the relevant source and then refreshes the corresponding pipeline:

```bash
databricks bundle run -t dev demo_01_variant_dimension
databricks bundle run -t dev demo_02_effective_dated_vat
databricks bundle run -t dev demo_03_weather_forecast
databricks bundle run -t dev demo_04_monitored_backfill
databricks bundle run -t dev demo_05_open_accounting_period
databricks bundle run -t dev demo_06_authoritative_slice
```

### 1. Bounded `VARIANT` dimension propagation

The initial load uses specification version 1 for all 60 fact dates. The demo changes product 1 to a richer nested JSON document and runs the MV with `recompute_days=7`.

Expected result: product 1 has the new specification only for the recent replacement horizon; older target rows still contain the previously materialized JSON and version.

Override the horizon at job run time:

```bash
databricks bundle run -t dev --params recompute_days=30 demo_01_variant_dimension
```

### 2. VAT effective from 1 January

The mutation changes the synthetic Czech standard rate effective `2026-01-01`. The demo job passes `vat_recompute_from=2026-01-01`.

Expected result: December 2025 keeps its historical rule and amount; January 2026 onward uses the corrected effective-dated rule.

A different legal boundary can be supplied for a run:

```bash
databricks bundle run -t dev --params vat_recompute_from=2026-02-01 demo_02_effective_dated_vat
```

### 3. Weather forecasts

The mutation publishes a later forecast for Prague tomorrow. The saved pipeline default is `weather_history_days=0`, which makes its replacement predicate equivalent to `forecast_date >= CURRENT_DATE()`.

The initializer temporarily uses `weather_history_days=3650` so yesterday is present in the target before the demonstration. The ordinary demo then returns to `0`.

Expected result: tomorrow changes to the new `wx-v3` issuance, while yesterday remains at the previously materialized issuance.

### 4. Monitored, parameterized backfill

`04_create_backfill_request.sql` changes source events 45–38 days ago and writes a `PENDING` row to `backfill_requests`. The monitor:

1. claims the request with a unique token;
2. starts pipeline 4 through `POST /api/2.0/pipelines/{pipeline_id}/updates`;
3. passes `backfill_start_date` and `backfill_end_date` in the update `parameters` map;
4. selects the fully qualified `customer_daily_metrics` dataset for refresh;
5. polls the update to a terminal state;
6. records `COMPLETED` or `FAILED`, the pipeline update ID, and any error message.

This keeps one transformation and one REPLACE flow. Backfill ranges are data-driven rather than hard-coded into additional flow definitions.

The standalone request body is in `examples/start_update_with_parameters.json`.

### 5. Open versus closed accounting periods

The mutation changes both June (`CLOSED`) and July (`OPEN`) allocations. The job uses `legal_entity=PL01` and `open_period_start=2026-07-01`.

Expected result: July is recomputed with allocation version 2, while the already-published June target retains version 1. To reopen June deliberately:

```bash
databricks bundle run -t dev \
  --params legal_entity=PL01,open_period_start=2026-06-01 \
  demo_05_open_accounting_period
```

### 6. Authoritative slice restatement

The source starts with PAYFAST file version 1 containing `T100` and `T200`. The demo accepts version 2 containing corrected `T100` and new `T250`, but no `T200`.

Expected result: the target day exactly matches version 2. `T200` disappears without explicit target-side delete logic.

Override the provider or rolling horizon as job parameters:

```bash
databricks bundle run -t dev \
  --params provider=PAYFAST,restatement_lookback_days=30 \
  demo_06_authoritative_slice
```

## Important operational detail

All four Streaming Table targets set:

```sql
TBLPROPERTIES ('pipelines.reset.allowed' = 'false')
```

A full reset of a bounded REPLACE target can discard history outside its current predicate. The property is included so accidental full refreshes fail rather than silently shrinking the demonstration table.

## Reset the demo

Re-running initialization deterministically recreates all source tables:

```bash
databricks bundle run -t dev initialize_all_examples
```

The bounded Streaming Table targets intentionally reject full reset. For a completely clean target history, drop the six output datasets first or deploy with a different `output_schema_base`.

## Local checks

Run the included structural checks before deployment:

```bash
python tests/static_checks.py
```

They parse all bundle YAML, verify the development schema-prefix setting, resolve every pipeline/job source reference, check exactly six pipelines and six mutation notebooks, confirm two MV plus four ST definitions, match SQL parameter references to pipeline defaults, verify Streaming Table reset protection, inspect the monitored-backfill API logic, and validate the example JSON payload.

Workspace-level bundle validation and SQL analysis are still required, particularly for the Materialized View preview syntax.
