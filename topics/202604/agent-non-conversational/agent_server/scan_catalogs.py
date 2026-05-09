"""Helpers for scanning Unity Catalog metadata and evaluating naming conventions."""

from __future__ import annotations

import io
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

try:  # pragma: no cover - imported in Databricks app runtime
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import sql
except Exception:  # pragma: no cover - keeps local static checks usable
    WorkspaceClient = Any  # type: ignore[assignment]
    sql = None  # type: ignore[assignment]


DEFAULT_ALLOWED_LAYERS = {"raw", "refined", "serving"}
DEFAULT_SCHEMA_FALLBACK_LAYER = "serving"
DEFAULT_TABLE_PREFIX = "tbl_"
DEFAULT_IDENTIFIER_SUFFIX = "_id"
DEFAULT_DATE_SUFFIX = "_dt"
DEFAULT_TIMESTAMP_SUFFIX = "_ts"
DEFAULT_BOOLEAN_PREFIX = "is_"


@dataclass(frozen=True)
class InformationSchemaRecord:
    table_catalog: str
    table_schema: str
    table_name: str
    column_name: str
    data_type: str


@dataclass(frozen=True)
class NamingRules:
    allowed_layers: set[str]
    schema_fallback_layer: str
    table_prefix: str
    identifier_suffix: str
    date_suffix: str
    timestamp_suffix: str
    boolean_prefix: str


class CatalogScanError(RuntimeError):
    """Raised when the information schema scan cannot be completed."""


def parse_target_catalogs(value: Sequence[str] | str | None) -> list[str]:
    """Normalize catalog input from request payload or environment variable."""
    if value is None:
        return []

    if isinstance(value, str):
        items = [item.strip() for item in value.split(",")]
    else:
        items = [str(item).strip() for item in value]

    catalogs: list[str] = []
    seen: set[str] = set()
    for item in items:
        if not item:
            continue
        normalized = item.strip()
        if normalized.lower() not in seen:
            catalogs.append(normalized)
            seen.add(normalized.lower())
    return catalogs


def load_skill_file(skill_file_path: str | None) -> str:
    """Load the enterprise naming skill file from the configured path."""
    if skill_file_path:
        p = Path(skill_file_path)
        if p.exists() and p.is_file():
            return p.read_text(encoding="utf-8")

    fallback = Path(__file__).resolve().parent.parent / "skills" / "enterprise-naming-convention" / "SKILL.md"
    if fallback.exists() and fallback.is_file():
        return fallback.read_text(encoding="utf-8")

    raise FileNotFoundError(
        f"Unable to locate SKILL.md. Checked: {skill_file_path}, {fallback}"
    )


def extract_naming_rules(skill_text: str) -> NamingRules:
    """Parse the loaded skill text into executable naming rules.

    Falls back to defaults if a rule cannot be extracted from the markdown.
    """
    allowed_layers = set(DEFAULT_ALLOWED_LAYERS)
    schema_fallback_layer = DEFAULT_SCHEMA_FALLBACK_LAYER
    table_prefix = DEFAULT_TABLE_PREFIX
    identifier_suffix = DEFAULT_IDENTIFIER_SUFFIX
    date_suffix = DEFAULT_DATE_SUFFIX
    timestamp_suffix = DEFAULT_TIMESTAMP_SUFFIX
    boolean_prefix = DEFAULT_BOOLEAN_PREFIX

    for raw_line in skill_text.splitlines():
        line = raw_line.strip()
        lower_line = line.lower()
        backticked_values = [v.strip() for v in re.findall(r"`([^`]+)`", line)]

        if "layer is one of" in lower_line and backticked_values:
            allowed_layers = {v.lower() for v in backticked_values if v}
            if schema_fallback_layer not in allowed_layers:
                schema_fallback_layer = next(iter(sorted(allowed_layers)), DEFAULT_SCHEMA_FALLBACK_LAYER)
        elif "convert it to <domain>_" in lower_line:
            match = re.search(r"<domain>_([a-z_]+)", lower_line)
            if match:
                schema_fallback_layer = match.group(1)
        elif "table names must start with" in lower_line and backticked_values:
            table_prefix = backticked_values[0]
        elif "identifiers end with" in lower_line and backticked_values:
            identifier_suffix = backticked_values[0]
        elif "date columns end with" in lower_line and backticked_values:
            date_suffix = backticked_values[0]
        elif "timestamp columns end with" in lower_line and backticked_values:
            timestamp_suffix = backticked_values[0]
        elif "boolean columns start with" in lower_line and backticked_values:
            boolean_prefix = backticked_values[0]

    return NamingRules(
        allowed_layers=allowed_layers,
        schema_fallback_layer=schema_fallback_layer,
        table_prefix=table_prefix,
        identifier_suffix=identifier_suffix,
        date_suffix=date_suffix,
        timestamp_suffix=timestamp_suffix,
        boolean_prefix=boolean_prefix,
    )


def adjust_validate_to_output_format(
    rows: Sequence[InformationSchemaRecord | dict[str, Any]],
    skill_text: str,
) -> list[dict[str, Any]]:
    """Convert raw information schema rows into the requested JSON report shape."""
    rules = extract_naming_rules(skill_text)
    formatted_rows: list[dict[str, Any]] = []

    for row in rows:
        record = row if isinstance(row, InformationSchemaRecord) else InformationSchemaRecord(
            table_catalog=str(row["table_catalog"]),
            table_schema=str(row["table_schema"]),
            table_name=str(row["table_name"]),
            column_name=str(row["column_name"]),
            data_type=str(row["data_type"]),
        )

        corrected_schema = normalize_schema_name(record.table_schema, rules)
        corrected_table = normalize_table_name(record.table_name, rules)
        corrected_column, column_reason = normalize_column_name(record.column_name, record.data_type, rules)

        reasons: list[str] = []
        if record.table_schema != corrected_schema:
            reasons.append(f"schema->{corrected_schema}")
        if record.table_name != corrected_table:
            reasons.append(f"table->{corrected_table}")
        if column_reason:
            reasons.append(column_reason)
        elif record.column_name != corrected_column:
            reasons.append(f"field->{corrected_column}")

        formatted_rows.append(
            {
                "table": f"{record.table_catalog}.{record.table_schema}.{record.table_name}",
                "field_original": {
                    "schema": record.table_schema,
                    "table": record.table_name,
                    "column": record.column_name,
                },
                "field_corrected": {
                    "schema": corrected_schema,
                    "table": corrected_table,
                    "column": corrected_column,
                },
                "check_result": "PASS" if not reasons else f"FAIL: {', '.join(reasons)}",
            }
        )

    return formatted_rows


def read_information_schema(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    catalogs: Sequence[str] | str | None,
) -> list[InformationSchemaRecord]:
    """Read catalog metadata from information_schema for the selected catalogs."""
    resolved_catalogs = parse_target_catalogs(catalogs)
    if not resolved_catalogs:
        raise CatalogScanError("No target catalogs were provided.")
    if not warehouse_id:
        raise CatalogScanError("SCAN_WAREHOUSE_ID is not configured.")
    if sql is None:
        raise CatalogScanError(
            "databricks-sdk is not available in the current runtime."
        )

    all_records: list[InformationSchemaRecord] = []
    for catalog_name in resolved_catalogs:
        schema_rows = _run_statement(
            workspace_client,
            warehouse_id,
            statement=(
                f"SELECT schema_name "
                f"FROM {_quote_identifier(catalog_name)}.information_schema.schemata "
                "WHERE schema_name <> 'information_schema' "
                "ORDER BY schema_name"
            ),
        )

        for schema_row in schema_rows:
            schema_name = str(schema_row[0])
            column_rows = _run_statement(
                workspace_client,
                warehouse_id,
                statement=(
                    f"SELECT table_catalog, table_schema, table_name, column_name, data_type "
                    f"FROM {_quote_identifier(catalog_name)}.information_schema.columns "
                    f"WHERE table_schema = {_quote_sql_string(schema_name)} "
                    f"ORDER BY table_name, ordinal_position"
                ),
            )

            for row in column_rows:
                if len(row) < 5:
                    raise CatalogScanError("Unexpected information_schema result shape.")
                all_records.append(
                    InformationSchemaRecord(
                        table_catalog=str(row[0]),
                        table_schema=str(row[1]),
                        table_name=str(row[2]),
                        column_name=str(row[3]),
                        data_type=str(row[4]),
                    )
                )

    return all_records


def save_report(
    workspace_client: WorkspaceClient,
    report_rows: Sequence[dict[str, Any]],
    volume_path: str,
    report_dir: str,
) -> str:
    """Persist the JSON report to the configured Unity Catalog volume."""
    if not volume_path:
        raise CatalogScanError("REPORT_VOLUME_PATH is not configured.")

    volume_root = _resolve_volume_path(volume_path)
    safe_report_dir = normalize_identifier(report_dir) or "naming_convention"
    output_dir = f"{volume_root}/{safe_report_dir}"

    try:
        workspace_client.files.create_directory(output_dir)
    except Exception as exc:
        raise CatalogScanError(
            f"Failed to create directory '{output_dir}': {exc}"
        ) from exc

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_file = f"{output_dir}/naming_convention_report_{timestamp}.json"
    content = json.dumps(list(report_rows), indent=2, ensure_ascii=False).encode("utf-8")

    try:
        workspace_client.files.upload(output_file, io.BytesIO(content), overwrite=True)
    except Exception as exc:
        raise CatalogScanError(
            f"Failed to upload report to '{output_file}': {exc}"
        ) from exc

    return output_file


# ---------------------------------------------------------------------------
# Normalisation helpers (all rules driven by NamingRules parsed from SKILL.md)
# ---------------------------------------------------------------------------

def normalize_schema_name(schema_name: str, rules: NamingRules) -> str:
    normalized = normalize_identifier(schema_name) or "schema"
    tokens = [t for t in normalized.split("_") if t]
    if len(tokens) >= 2 and tokens[-1] in rules.allowed_layers:
        return "_".join(tokens)
    return f"{normalized}_{rules.schema_fallback_layer}"


def normalize_table_name(table_name: str, rules: NamingRules) -> str:
    normalized = normalize_identifier(table_name) or "table"
    if normalized.startswith(rules.table_prefix):
        return normalized
    return f"{rules.table_prefix}{normalized}"


def normalize_column_name(column_name: str, data_type: str, rules: NamingRules) -> tuple[str, str | None]:
    normalized = normalize_identifier(column_name) or "column"
    dt = data_type.upper()

    if "BOOLEAN" in dt:
        if not normalized.startswith(rules.boolean_prefix):
            return f"{rules.boolean_prefix}{normalized}", None
    elif "TIMESTAMP" in dt:
        normalized = _strip_suffixes(normalized, ("_timestamp", "_ts", "_time"))
        if not normalized.endswith(rules.timestamp_suffix):
            return f"{normalized}{rules.timestamp_suffix}", None
    elif dt == "DATE":
        normalized = _strip_suffixes(normalized, ("_date", "_dt"))
        if not normalized.endswith(rules.date_suffix):
            return f"{normalized}{rules.date_suffix}", None
    elif _looks_like_identifier(column_name, normalized):
        result, reason = _ensure_identifier_suffix(normalized, rules.identifier_suffix)
        if reason:
            return result, f"field->{result} (ambiguous identifier)"
        if result != normalized:
            return result, None

    return normalized, None


def normalize_identifier(value: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        return ""
    cleaned = cleaned.replace("&", " and ")
    cleaned = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", cleaned)
    cleaned = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", cleaned)
    cleaned = re.sub(r"[\s\-/.]+", "_", cleaned)
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned)
    return cleaned.strip("_").lower()


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _strip_suffixes(value: str, suffixes: tuple[str, ...]) -> str:
    for s in suffixes:
        if value.endswith(s):
            return value[: -len(s)].rstrip("_")
    return value


def _looks_like_identifier(original: str, normalized: str) -> bool:
    stripped = re.sub(r"[^A-Za-z0-9]", "", original)
    return bool(
        normalized.endswith("_id")
        or normalized.endswith("_identifier")
        or normalized.endswith("_key")
        or re.search(r"(?:id|ID|Id)$", stripped)
    )


def _ensure_identifier_suffix(value: str, suffix: str) -> tuple[str, str | None]:
    if value == suffix.lstrip("_"):
        return value, "identifier"
    if value.endswith(suffix):
        return value, None
    for old in ("_identifier", "_key"):
        if value.endswith(old):
            return f"{value[:-len(old)]}{suffix}", None
    if value.endswith("id") and not value.endswith(suffix) and len(value) > 2:
        prefix = value[:-2].rstrip("_")
        if prefix:
            return f"{prefix}{suffix}", None
    return f"{value}{suffix}", None


def _run_statement(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    statement: str,
) -> list[list[Any]]:
    api = workspace_client.statement_execution
    execute_fn = getattr(api, "execute_statement", None) or getattr(api, "execute", None)
    if execute_fn is None:
        raise CatalogScanError("SDK does not expose statement execution.")

    response = execute_fn(
        statement=statement,
        warehouse_id=warehouse_id,
        disposition=sql.Disposition.INLINE,
        format=sql.Format.JSON_ARRAY,
        wait_timeout="50s",
    )

    if callable(getattr(response, "result", None)):
        response = response.result()

    response = _wait_for_completion(workspace_client, response)
    state = _state_name(getattr(getattr(response, "status", None), "state", None))
    if state != "SUCCEEDED":
        raise CatalogScanError(_statement_error_message(response))

    result = getattr(response, "result", None)
    rows: list[list[Any]] = list(getattr(result, "data_array", None) or [])
    next_chunk = getattr(result, "next_chunk_index", None)
    stmt_id = getattr(response, "statement_id", None)

    while next_chunk is not None and stmt_id:
        chunk = api.get_statement_result_chunk_n(statement_id=stmt_id, chunk_index=next_chunk)
        rows.extend(chunk.data_array or [])
        next_chunk = chunk.next_chunk_index

    return rows


def _wait_for_completion(workspace_client: WorkspaceClient, response: Any, timeout: int = 120) -> Any:
    deadline = time.time() + timeout
    stmt_id = getattr(response, "statement_id", None)
    state = _state_name(getattr(getattr(response, "status", None), "state", None))

    terminal = {"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"}
    while state not in terminal and stmt_id:
        if time.time() > deadline:
            raise CatalogScanError(f"Timed out waiting for statement {stmt_id}.")
        time.sleep(1)
        response = workspace_client.statement_execution.get_statement(statement_id=stmt_id)
        state = _state_name(getattr(getattr(response, "status", None), "state", None))

    return response


def _state_name(state: Any) -> str:
    if state is None:
        return "UNKNOWN"
    return str(getattr(state, "value", state)).upper()


def _statement_error_message(response: Any) -> str:
    status = getattr(response, "status", None)
    state = _state_name(getattr(status, "state", None))
    error = getattr(status, "error", None)
    if error is None:
        return f"SQL statement failed with state {state}."
    message = getattr(error, "message", None) or getattr(error, "error_message", None)
    code = getattr(error, "error_code", None)
    return ": ".join(p for p in [f"Failed ({state})", code, message] if p)


def _quote_identifier(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


def _quote_sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _resolve_volume_path(volume_path: str) -> str:
    raw = volume_path.strip()
    if raw.startswith("dbfs:/Volumes/"):
        raw = raw.replace("dbfs:/", "/", 1)
    if raw.startswith("/Volumes/"):
        return raw.rstrip("/")
    parts = [p.strip() for p in raw.split(".") if p.strip()]
    if len(parts) == 3:
        return f"/Volumes/{parts[0]}/{parts[1]}/{parts[2]}"
    return raw
