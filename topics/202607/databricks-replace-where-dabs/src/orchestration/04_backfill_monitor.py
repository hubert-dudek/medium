# Databricks notebook source
"""Consume pending backfill requests and run a parameterized pipeline update.

The request table is deliberately outside the pipeline. This notebook is run by
Lakeflow Jobs, claims each request, starts the same REPLACE WHERE pipeline with
start/end parameter overrides, waits for completion, and records the result.
"""

# COMMAND ----------
import json
import re
import time
from datetime import datetime, timezone
from uuid import uuid4

from databricks.sdk import WorkspaceClient

# COMMAND ----------
dbutils.widgets.text("pipeline_id", "")
dbutils.widgets.text("source_catalog", "main")
dbutils.widgets.text("source_schema", "replace_where_demo_src_dev")
dbutils.widgets.text("output_catalog", "main")
dbutils.widgets.text("output_schema", "replace_where_demo_out_dev")
dbutils.widgets.text("target_dataset", "customer_daily_metrics")
dbutils.widgets.text("max_requests", "10")
dbutils.widgets.text("poll_seconds", "10")
dbutils.widgets.text("timeout_seconds", "3600")

pipeline_id = dbutils.widgets.get("pipeline_id").strip()
source_catalog = dbutils.widgets.get("source_catalog").strip()
source_schema = dbutils.widgets.get("source_schema").strip()
output_catalog = dbutils.widgets.get("output_catalog").strip()
output_schema = dbutils.widgets.get("output_schema").strip()
target_dataset = dbutils.widgets.get("target_dataset").strip()
max_requests = int(dbutils.widgets.get("max_requests"))
poll_seconds = int(dbutils.widgets.get("poll_seconds"))
timeout_seconds = int(dbutils.widgets.get("timeout_seconds"))

if not pipeline_id:
    raise ValueError("pipeline_id must be supplied by the bundle job")
if max_requests < 1:
    raise ValueError("max_requests must be at least 1")
if poll_seconds < 1 or timeout_seconds < 1:
    raise ValueError("poll_seconds and timeout_seconds must be positive")

_SIMPLE_DATASET = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
if not _SIMPLE_DATASET.fullmatch(target_dataset):
    raise ValueError("target_dataset must be an unqualified SQL identifier")


def quote_identifier(value: str) -> str:
    """Backtick-quote one catalog, schema, table, or flow-name component."""
    if not value:
        raise ValueError("Identifier cannot be empty")
    return "`" + value.replace("`", "``") + "`"


def qualify_identifier(*parts: str) -> str:
    """Return a fully qualified SQL identifier."""
    return ".".join(quote_identifier(part) for part in parts)


def api_identifier_component(value: str) -> str:
    """Format one component for an API dataset name.

    Simple components remain unquoted, producing the familiar
    ``catalog.schema.table`` form. Components that require quoting retain SQL
    backticks, which also preserves embedded special characters.
    """
    if _SIMPLE_DATASET.fullmatch(value):
        return value
    return quote_identifier(value)


def qualify_api_dataset(*parts: str) -> str:
    """Return a fully qualified dataset name for refresh_selection."""
    return ".".join(api_identifier_component(part) for part in parts)


request_table = qualify_identifier(source_catalog, source_schema, "backfill_requests")
target_flow = qualify_api_dataset(output_catalog, output_schema, target_dataset)

# COMMAND ----------
requests = spark.sql(
    f"""
    SELECT request_id, start_date, end_date, reason, requested_at
    FROM {request_table}
    WHERE status = 'PENDING'
    ORDER BY requested_at, request_id
    LIMIT {max_requests}
    """
).collect()

if not requests:
    print("No pending backfill requests.")
    dbutils.notebook.exit(json.dumps({"processed": 0, "status": "NO_PENDING_REQUESTS"}))

client = WorkspaceClient()
terminal_states = {"COMPLETED", "FAILED", "CANCELED"}
processed = []

# COMMAND ----------
for request in requests:
    request_id = request["request_id"]
    start_date = request["start_date"]
    end_date = request["end_date"]

    if start_date is None or end_date is None or start_date > end_date:
        spark.sql(
            f"""
            UPDATE {request_table}
            SET status = 'FAILED',
                completed_at = CURRENT_TIMESTAMP(),
                error_message = :error_message
            WHERE request_id = :request_id AND status = 'PENDING'
            """,
            args={
                "request_id": request_id,
                "error_message": "Invalid request: start_date must be on or before end_date",
            },
        )
        processed.append({"request_id": request_id, "state": "FAILED_VALIDATION"})
        continue

    # Atomic claim: only this monitor's unique token is accepted afterwards.
    claim_token = str(uuid4())
    spark.sql(
        f"""
        UPDATE {request_table}
        SET status = 'RUNNING',
            started_at = CURRENT_TIMESTAMP(),
            completed_at = NULL,
            pipeline_update_id = NULL,
            claim_token = :claim_token,
            error_message = NULL
        WHERE request_id = :request_id AND status = 'PENDING'
        """,
        args={"request_id": request_id, "claim_token": claim_token},
    )

    claimed = spark.sql(
        f"""
        SELECT COUNT(*) AS claimed
        FROM {request_table}
        WHERE request_id = :request_id
          AND status = 'RUNNING'
          AND claim_token = :claim_token
        """,
        args={"request_id": request_id, "claim_token": claim_token},
    ).first()["claimed"]

    if claimed != 1:
        processed.append({"request_id": request_id, "state": "SKIPPED_NOT_CLAIMED"})
        continue

    try:
        body = {
            "cause": "JOB_TASK",
            "parameters": {
                "source_catalog": source_catalog,
                "source_schema": source_schema,
                "backfill_start_date": start_date.isoformat(),
                "backfill_end_date": end_date.isoformat(),
            },
            "refresh_selection": [target_flow],
        }

        response = client.api_client.do(
            "POST",
            f"/api/2.0/pipelines/{pipeline_id}/updates",
            body=body,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )
        update_id = response["update_id"]

        spark.sql(
            f"""
            UPDATE {request_table}
            SET pipeline_update_id = :update_id
            WHERE request_id = :request_id
            """,
            args={"request_id": request_id, "update_id": update_id},
        )

        started_monotonic = time.monotonic()
        state = "QUEUED"
        while state not in terminal_states:
            if time.monotonic() - started_monotonic > timeout_seconds:
                raise TimeoutError(
                    f"Pipeline update {update_id} did not finish within {timeout_seconds} seconds"
                )

            time.sleep(poll_seconds)
            status_response = client.api_client.do(
                "GET",
                f"/api/2.0/pipelines/{pipeline_id}/updates/{update_id}",
                headers={"Accept": "application/json"},
            )
            state = status_response["update"]["state"]
            print(f"request={request_id} update={update_id} state={state}")

        if state != "COMPLETED":
            raise RuntimeError(f"Pipeline update {update_id} ended in state {state}")

        spark.sql(
            f"""
            UPDATE {request_table}
            SET status = 'COMPLETED',
                completed_at = CURRENT_TIMESTAMP(),
                error_message = NULL
            WHERE request_id = :request_id
            """,
            args={"request_id": request_id},
        )
        processed.append(
            {"request_id": request_id, "state": state, "update_id": update_id}
        )

    except Exception as exc:
        error_message = f"{type(exc).__name__}: {exc}"[:4000]
        spark.sql(
            f"""
            UPDATE {request_table}
            SET status = 'FAILED',
                completed_at = CURRENT_TIMESTAMP(),
                error_message = :error_message
            WHERE request_id = :request_id
            """,
            args={"request_id": request_id, "error_message": error_message},
        )
        processed.append(
            {"request_id": request_id, "state": "FAILED", "error": error_message}
        )
        # Continue to later requests; the final summary makes failures visible.

# COMMAND ----------
summary = {
    "processed": len(processed),
    "finished_at": datetime.now(timezone.utc).isoformat(),
    "requests": processed,
}
print(json.dumps(summary, indent=2))

if any(item["state"].startswith("FAILED") for item in processed):
    raise RuntimeError("One or more backfill requests failed: " + json.dumps(summary))

dbutils.notebook.exit(json.dumps(summary))
