import argparse
import configparser
import datetime as dt
import decimal
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time


UTC = dt.timezone.utc
EXPECTED_HOST = "https://dbc-ca93cffe-fba0.cloud.databricks.com"
EXPECTED_WORKSPACE_ID = "3665629714053906"
EXPECTED_IDENTITY = "hdude2020@gmail.com"
TABLES = {
    "gateway": "system.ai_gateway.usage",
    "billing": "system.billing.usage",
    "prices": "system.billing.list_prices",
}


def timestamp(value):
    if not value:
        return None
    parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Manifest timestamps must include a timezone")
    return parsed.astimezone(UTC)


def iso(value):
    return value.isoformat().replace("+00:00", "Z") if value else None


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def split_fields(value):
    fields, start, depth, quoted = [], 0, 0, False
    for index, char in enumerate(value):
        if char == "`":
            quoted = not quoted
        elif not quoted:
            depth += char in "<("
            depth -= char in ">)"
            if char == "," and depth == 0:
                fields.append(value[start:index].strip())
                start = index + 1
    fields.append(value[start:].strip())
    return fields


def schema_paths(rows):
    paths = set()

    def add(name, datatype):
        paths.add(name.lower())
        datatype = datatype.strip()
        if datatype.lower().startswith("struct<") and datatype.endswith(">"):
            for field in split_fields(datatype[7:-1]):
                key, separator, nested_type = field.partition(":")
                if separator:
                    add(name + "." + key.strip().strip("`"), nested_type)

    for row in rows:
        name = str(row.get("col_name") or "").strip()
        if name and not name.startswith("#"):
            add(name, str(row.get("data_type") or ""))
    return paths


def decoded(value):
    if not isinstance(value, str):
        return value
    if value.startswith(("{", "[")):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def nested(row, path):
    value = row
    for key in path.split("."):
        value = decoded(value)
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


class Databricks:
    def __init__(self, cli, profile, config_file, warehouse_id, timeout, run_id):
        self.cli = cli
        self.profile = profile
        self.warehouse_id = warehouse_id
        self.timeout = timeout
        self.run_id = run_id
        self.statements = []
        self.env = os.environ.copy()
        for name in (
            "DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_USERNAME",
            "DATABRICKS_PASSWORD", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET",
            "DATABRICKS_ACCOUNT_ID", "DATABRICKS_AUTH_TYPE", "DATABRICKS_CONFIG_PROFILE",
        ):
            self.env.pop(name, None)
        if config_file:
            self.env["DATABRICKS_CONFIG_FILE"] = config_file

    def api(self, method, path, body=None):
        command = [self.cli, "api", method, path, "--profile", self.profile, "--output", "json"]
        temporary_path = None
        try:
            if body is not None:
                with tempfile.NamedTemporaryFile(mode="w", suffix=".json", encoding="utf-8", delete=False) as handle:
                    json.dump(body, handle)
                    temporary_path = handle.name
                command.extend(["--json", "@" + temporary_path])
            result = subprocess.run(command, env=self.env, capture_output=True, text=True,
                                    encoding="utf-8", timeout=self.timeout, check=False)
            if result.returncode:
                raise RuntimeError("Databricks CLI request failed with exit code " + str(result.returncode))
            return json.loads(result.stdout) if result.stdout.strip() else {}
        finally:
            if temporary_path:
                Path(temporary_path).unlink(missing_ok=True)

    def query(self, statement):
        payload = {
            "warehouse_id": self.warehouse_id,
            "statement": statement,
            "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
            "row_limit": 10000,
            "query_tags": [{"key": "run_id", "value": self.run_id},
                           {"key": "phase", "value": "cost_reconciliation"}],
        }
        response = self.api("post", "/api/2.0/sql/statements", payload)
        statement_id = response.get("statement_id")
        if statement_id:
            self.statements.append(statement_id)
        deadline = time.monotonic() + self.timeout
        while response.get("status", {}).get("state") in {"PENDING", "RUNNING"}:
            if time.monotonic() >= deadline:
                if statement_id:
                    self.api("post", "/api/2.0/sql/statements/" + statement_id + "/cancel", {})
                raise TimeoutError("Reconciliation SQL exceeded its time limit")
            time.sleep(2)
            response = self.api("get", "/api/2.0/sql/statements/" + statement_id)
        status = response.get("status", {})
        if status.get("state") != "SUCCEEDED":
            error = status.get("error", {})
            raise RuntimeError("SQL failed: " + str(error.get("error_code") or status.get("state")))
        manifest = response.get("manifest", {})
        if manifest.get("truncated"):
            raise RuntimeError("SQL result was truncated; narrow the reconciliation interval")
        columns = [column["name"] for column in manifest.get("schema", {}).get("columns", [])]
        rows, chunk = [], response.get("result", {})
        while chunk:
            rows.extend(dict(zip(columns, [decoded(value) for value in values]))
                        for values in chunk.get("data_array", []))
            next_link = chunk.get("next_chunk_internal_link")
            chunk = self.api("get", next_link) if next_link else None
        return rows


def model_services(manifest):
    items = manifest.get("model_services") or manifest.get("services") or []
    if isinstance(items, dict):
        items = list(items.values())
    names, identifiers = [], []
    for item in items:
        if isinstance(item, str):
            name, identifier = item, None
        else:
            name = item.get("name") or item.get("service_name") or item.get("full_name")
            identifier = item.get("id") or item.get("service_id") or item.get("endpoint_id")
        name = str(name or "").removeprefix("model-services/")
        if name and not name.startswith("system.ai."):
            names.append(name)
            if identifier:
                identifiers.append(str(identifier))
    return sorted(set(names)), sorted(set(identifiers))


def in_values(field, values):
    return field + " IN (" + ",".join(literal(value) for value in values) + ")"


def exact_predicate(paths, manifest, kind):
    run_id = manifest["run_id"]
    names, identifiers = model_services(manifest)
    predicates = []
    tag_column = "request_tags" if kind == "gateway" else "custom_tags"
    tag_keys = manifest.get("run_tag_keys") or ["run_id", "benchmark_run", "benchmark_run_id"]
    if tag_column in paths:
        predicates.extend(tag_column + "[" + literal(key) + "] = " + literal(run_id) for key in tag_keys)
    name_fields = ["service_name", "endpoint_name"] if kind == "gateway" else ["usage_metadata.ai_gateway.endpoint_name"]
    id_fields = ["service_id", "endpoint_id"] if kind == "gateway" else ["usage_metadata.ai_gateway.endpoint_id"]
    if names:
        predicates.extend(in_values(field, names) for field in name_fields if field in paths)
    if identifiers:
        predicates.extend(in_values(field, identifiers) for field in id_fields if field in paths)
    return "(" + " OR ".join(predicates) + ")" if predicates else None


def time_predicate(paths, start, end, gateway=False):
    if "usage_start_time" in paths and "usage_end_time" in paths:
        return ("usage_start_time < TIMESTAMP " + literal(iso(end))
                + " AND usage_end_time > TIMESTAMP " + literal(iso(start)))
    if gateway:
        for field in ("request_time", "event_time", "timestamp", "request_timestamp"):
            if field in paths:
                return (field + " >= TIMESTAMP " + literal(iso(start))
                        + " AND " + field + " <= TIMESTAMP " + literal(iso(end)))
    return None


def selected_columns(paths, kind):
    candidates = {
        "gateway": ["account_id", "workspace_id", "request_id", "databricks_request_id", "request_time",
                    "event_time", "timestamp", "request_timestamp", "service_id", "service_name", "service_tags",
                    "endpoint_id", "endpoint_name", "endpoint_tags", "request_tags", "requester", "requester_id",
                    "status_code", "http_status_code", "input_tokens", "output_tokens", "total_tokens",
                    "input_token_count", "output_token_count", "total_token_count",
                    "latency_ms", "latency", "destination_model", "destination_id", "token_details", "invocation_metadata"],
        "billing": ["record_id", "account_id", "workspace_id", "sku_name", "cloud", "usage_start_time",
                    "usage_end_time", "usage_date", "custom_tags", "usage_unit", "usage_quantity", "usage_metadata",
                    "identity_metadata", "record_type", "ingestion_date", "billing_origin_product", "product_features"],
        "prices": ["account_id", "price_start_time", "price_end_time", "sku_name", "cloud", "currency_code",
                   "usage_unit", "pricing"],
    }
    return [column for column in candidates[kind] if column in paths]


def collect(client, paths, table, predicate, kind):
    columns = selected_columns(paths, kind)
    if not columns or not predicate:
        return {"status": "unknown", "reason": "Required schema fields are unavailable", "rows": []}
    try:
        rows = client.query("SELECT " + ", ".join(columns) + " FROM " + table + " WHERE " + predicate)
        return {"status": "observed" if rows else "no_records_observed", "rows": rows,
                "row_count": len(rows), "predicate": predicate}
    except (RuntimeError, TimeoutError, subprocess.TimeoutExpired, ValueError) as error:
        return {"status": "unknown", "reason": str(error), "rows": [], "predicate": predicate}


def money_summary(rows, prices, age_hours):
    missing_state = "pending" if age_hours < 12 else "unknown"
    if not rows:
        return {"status": missing_state, "observed_list_cost_usd": None,
                "reason": "No matching billing records; absence is not evidence of zero cost"}
    total = decimal.Decimal(0)
    unpriced, matched = [], 0
    for row in rows:
        usage_start = timestamp(row.get("usage_start_time"))
        usage_end = timestamp(row.get("usage_end_time"))
        matches = []
        if usage_start and usage_end and usage_start < usage_end:
            for price in prices:
                if any(row.get(field) != price.get(field) for field in ("sku_name", "cloud", "usage_unit")):
                    continue
                if price.get("currency_code") != "USD":
                    continue
                lower, upper = timestamp(price.get("price_start_time")), timestamp(price.get("price_end_time"))
                if lower and lower <= usage_start and (upper is None or usage_end <= upper):
                    matches.append(price)
        rate = nested(matches[0], "pricing.effective_list.default") if len(matches) == 1 else None
        try:
            quantity = decimal.Decimal(str(row.get("usage_quantity")))
            if rate is None:
                raise decimal.InvalidOperation
            amount = quantity * decimal.Decimal(str(rate))
            row["effective_list_unit_price_usd"] = str(rate)
            row["observed_list_cost_usd"] = str(amount)
            total += amount
            matched += 1
        except decimal.InvalidOperation:
            row.pop("effective_list_unit_price_usd", None)
            row["observed_list_cost_usd"] = None
            unpriced.append(row.get("record_id"))
    return {
        "status": "partial" if unpriced else "observed_provisional",
        "observed_list_cost_usd": str(total) if matched else None,
        "priced_record_count": matched,
        "unpriced_record_ids": unpriced,
        "includes_signed_corrections": True,
        "is_final_invoice_cost": False,
        "completeness": "pending_delivery" if age_hours < 12 else "not_guaranteed",
    }


def reconcile(manifest, client, now):
    start = timestamp(manifest.get("started_at") or manifest.get("start_time"))
    finish = timestamp(manifest.get("finished_at") or manifest.get("end_time"))
    if start is None:
        raise ValueError("Manifest needs started_at or start_time")
    end = finish or now
    if end < start:
        raise ValueError("Manifest end time precedes start time")
    age_hours = max(0, (now - end).total_seconds() / 3600)
    report = {
        "run_id": manifest["run_id"], "checked_at": iso(now), "profile": client.profile,
        "window": {"start": iso(start), "end": iso(end), "end_inferred": finish is None},
        "billing_delivery_reference_hours": 12,
        "schemas": {}, "queries_generate_separate_sql_compute_cost": True,
        "benchmark_completion_status": manifest.get("status"),
    }
    schemas = {}
    for name, table in TABLES.items():
        try:
            description = client.query("DESCRIBE TABLE " + table)
            schemas[name] = schema_paths(description)
            report["schemas"][name] = {"status": "inspected", "paths": sorted(schemas[name])}
        except (RuntimeError, TimeoutError, subprocess.TimeoutExpired, ValueError) as error:
            schemas[name] = set()
            report["schemas"][name] = {"status": "unknown", "reason": str(error)}
    for name in ("gateway", "billing"):
        paths = schemas[name]
        predicate = exact_predicate(paths, manifest, name)
        if name == "billing":
            if predicate and "billing_origin_product" in paths:
                predicate += " AND billing_origin_product <> 'SQL'"
            else:
                predicate = None
        window = time_predicate(paths, start, end, gateway=name == "gateway")
        if predicate and window:
            predicate += " AND " + window
        workspace_id = manifest.get("workspace_id")
        if predicate and workspace_id and "workspace_id" in paths:
            predicate += " AND workspace_id = " + literal(workspace_id)
        report[name + "_exact"] = collect(client, paths, TABLES[name], predicate, name)
    paths = schemas["billing"]
    window = time_predicate(paths, start, end)
    workspace = ("workspace_id = " + literal(manifest["workspace_id"])) if manifest.get("workspace_id") and "workspace_id" in paths else None
    ai_parts = [window, workspace]
    if "billing_origin_product" in paths:
        ai_parts.append("billing_origin_product = 'AI_FUNCTIONS'")
    else:
        ai_parts.append(None)
    function_field = "product_features.ai_functions.ai_function"
    if function_field in paths:
        ai_parts.append(function_field + " = 'AI_DECIDE'")
    ai_predicate = " AND ".join(ai_parts) if all(ai_parts) else None
    report["ai_decide_candidates"] = collect(client, paths, TABLES["billing"], ai_predicate, "billing")
    report["ai_decide_candidates"]["attribution"] = "candidate_only_workspace_and_time"
    report["ai_decide_candidates"]["function_filter_available"] = function_field in paths
    batch_parts = [window, workspace]
    offering_field = "product_features.model_serving.offering_type"
    if "billing_origin_product" in paths and offering_field in paths:
        batch_parts.extend(["billing_origin_product = 'MODEL_SERVING'", offering_field + " = 'BATCH_INFERENCE'"])
    else:
        batch_parts.append(None)
    batch_predicate = " AND ".join(batch_parts) if all(batch_parts) else None
    report["batch_inference_candidates"] = collect(client, paths, TABLES["billing"], batch_predicate, "billing")
    report["batch_inference_candidates"]["attribution"] = "candidate_only_may_include_other_ai_functions"
    overhead_parts = [window, workspace]
    if "usage_metadata.warehouse_id" in paths and "billing_origin_product" in paths:
        overhead_parts.extend(["billing_origin_product = 'SQL'", "usage_metadata.warehouse_id = " + literal(client.warehouse_id)])
    else:
        overhead_parts.append(None)
    overhead = " AND ".join(overhead_parts) if all(overhead_parts) else None
    report["sql_compute_candidates"] = collect(client, paths, TABLES["billing"], overhead, "billing")
    report["sql_compute_candidates"]["attribution"] = "shared_warehouse_window_not_exact_per_query"
    billing_groups = ("billing_exact", "ai_decide_candidates", "batch_inference_candidates", "sql_compute_candidates")
    all_rows = [row for name in billing_groups for row in report[name]["rows"]]
    skus = sorted({row["sku_name"] for row in all_rows if row.get("sku_name")})
    price_paths = schemas["prices"]
    price_predicate = None
    if skus and {"sku_name", "price_start_time", "price_end_time", "currency_code"}.issubset(price_paths):
        price_start = min([start] + [timestamp(row["usage_start_time"]) for row in all_rows if row.get("usage_start_time")])
        price_end = max([end] + [timestamp(row["usage_end_time"]) for row in all_rows if row.get("usage_end_time")])
        price_predicate = (in_values("sku_name", skus) + " AND currency_code = 'USD'"
                           + " AND price_start_time <= TIMESTAMP " + literal(iso(price_end))
                           + " AND (price_end_time IS NULL OR price_end_time >= TIMESTAMP " + literal(iso(price_start)) + ")")
    report["prices"] = collect(client, price_paths, TABLES["prices"], price_predicate, "prices")
    if not skus:
        report["prices"]["reason"] = "No observed billing SKUs to price"
    for name in billing_groups:
        summary = money_summary(report[name]["rows"], report["prices"]["rows"], age_hours)
        if report[name]["status"] == "unknown":
            summary["status"] = "unknown"
            summary["reason"] = report[name].get("reason")
        report[name]["cost"] = summary
    report["combined_total_usd"] = None
    report["combined_total_reason"] = "Candidate costs can overlap exact records and include unrelated shared compute; do not add them"
    report["reconciliation_statement_ids"] = client.statements
    report["next_check_recommended_at"] = iso(max(now + dt.timedelta(hours=1), end + dt.timedelta(hours=12)))
    return report


def private_path(path, repository):
    resolved = path.expanduser().resolve()
    if resolved == repository or repository in resolved.parents:
        raise ValueError("Private manifests and cost output must be outside the Git repository")
    return resolved


def verify_context(manifest, client, config_file, credit_state):
    if manifest.get("host", "").rstrip("/") != EXPECTED_HOST:
        raise ValueError("Manifest host does not match AWS-DEV")
    if str(manifest.get("workspace_id")) != EXPECTED_WORKSPACE_ID:
        raise ValueError("Manifest workspace does not match AWS-DEV")
    settings = configparser.ConfigParser(interpolation=None)
    if not settings.read(config_file, encoding="utf-8-sig"):
        raise ValueError("Databricks CLI configuration is unavailable")
    if settings.get(client.profile, "host", fallback="").rstrip("/") != EXPECTED_HOST:
        raise ValueError("CLI profile host does not match AWS-DEV")
    state = json.loads(Path(credit_state).read_text(encoding="utf-8-sig"))
    workspace = state.get("workspaces", {}).get(client.profile, {})
    if str(workspace.get("workspace_id")) != EXPECTED_WORKSPACE_ID:
        raise ValueError("Credit state does not identify AWS-DEV")
    if workspace.get("exhausted_hold") is not False:
        raise ValueError("AWS-DEV exhausted-credit hold is active or unknown")
    observation = workspace.get("last_observation", {})
    try:
        balance = decimal.Decimal(str(observation.get("remaining_usd")))
        positive = balance.is_finite() and balance > 0
    except decimal.InvalidOperation:
        positive = False
    if not positive:
        raise ValueError("AWS-DEV has no saved positive credit observation")
    identity = client.api("get", "/api/2.0/preview/scim/v2/Me")
    if identity.get("userName") != EXPECTED_IDENTITY or identity.get("active") is not True:
        raise ValueError("Connected identity does not match active AWS-DEV owner")
    return {"host": EXPECTED_HOST, "workspace_id": EXPECTED_WORKSPACE_ID,
            "identity": {key: identity.get(key) for key in ("id", "userName", "active")},
            "credit_observed_at": observation.get("observed_at"), "saved_credit_hold": False}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--profile", default="AWS-DEV")
    parser.add_argument("--warehouse-id", default="8a19c545d43c9bc3")
    parser.add_argument("--cli", default=shutil.which("databricks") or r"C:\git\cli\databricks.exe")
    parser.add_argument("--config-file", default=r"C:\git\cli\.databrickscfg")
    parser.add_argument("--credit-state", default=r"C:\git\cli\.credit-monitor\state.json")
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    repository = Path(__file__).resolve().parents[3]
    manifest_path = private_path(args.manifest, repository)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    if not manifest.get("run_id"):
        raise ValueError("Manifest requires a nonempty run_id")
    now = dt.datetime.now(UTC)
    output = private_path(args.output or manifest_path.with_name("cost_reconciliation_" + now.strftime("%Y%m%dT%H%M%SZ") + ".json"), repository)
    if args.profile != "AWS-DEV":
        raise ValueError("This benchmark reconciler is scoped to AWS-DEV")
    if not args.execute:
        print(json.dumps({"status": "dry_run", "run_id": manifest["run_id"], "profile": args.profile,
                          "warehouse_id": args.warehouse_id, "tables": TABLES,
                          "output": str(output), "remote_queries_executed": False,
                          "next_action": "Use --execute after confirming workspace credits"}, indent=2))
        return
    client = Databricks(args.cli, args.profile, args.config_file, args.warehouse_id, args.timeout, manifest["run_id"])
    context = verify_context(manifest, client, args.config_file, args.credit_state)
    report = reconcile(manifest, client, now)
    report["verified_context"] = context
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"run_id": manifest["run_id"], "output": str(output),
                      "exact_billing_cost": report["billing_exact"]["cost"],
                      "ai_decide_candidate_cost": report["ai_decide_candidates"]["cost"],
                      "sql_compute_candidate_cost": report["sql_compute_candidates"]["cost"]}, indent=2))


if __name__ == "__main__":
    main()
