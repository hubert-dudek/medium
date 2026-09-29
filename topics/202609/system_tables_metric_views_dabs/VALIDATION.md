# Validation report

Prepared on 2026-09-22.

## Passed locally

**29 tests passed** using `python -m unittest discover -s tests -v`.

The tests parse every bundle YAML, dashboard JSON and Python source; inspect six persistent metric-view YAML definitions; verify exact schema names, warehouse lookup, deployment-run wiring, SQL notebook paths and task order; check materialization grains/measures and canonical-model reuse; and verify that parameter/refresh labs are opt-in.

Reference fixtures exercise signed billing corrections, missing prices, currency/unit/account/cloud separation, full-interval price matching, overlapping-price rejection, the raw fact-join fanout pitfall, March fiscal boundaries, leap-day alignment and expected month offsets. These fixtures run in Python and do not substitute for execution of the Spark SQL.

The dashboard test checks its one-dataset/one-chart structure and metric SQL references. It is not validation against the remote dashboard API's complete schema.

## Not executed in this environment

- `databricks bundle validate` with an authenticated workspace and actual CLI warehouse lookup.
- SQL DDL, metric analysis or queries on your Databricks compute.
- Managed materialization creation, refresh success, incremental/full-refresh behavior, or optimizer rewrite.
- Dashboard import, rendering or publication.
- The optional native-parameter lab on 18.2+ compute.

No Databricks workspace connection or CLI was available for those checks. No claim of a successful live deployment is made.

## Checks supplied for your deployment

The deployment job's preflight reads the required system-table columns, rejects overlapping USD price intervals, checks March fiscal boundaries and executes a temporary native metric view with dated offsets and a fiscal-YTD window. Unsupported warehouse features or missing source privileges therefore fail visibly.

The last task queries all six persistent metric views and displays the materialized definitions' extended metadata. Inspect its unpriced-record results before interpreting spend. Run `notebooks/20_materialization.py` after the initial managed refresh to inspect the physical plan; a successful source-fallback query is not proof of materialization use.

A successful SQL deployment does not guarantee complete billing history, invoice agreement, complete regional query telemetry or fully refreshed materializations.
