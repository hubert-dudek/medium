import argparse
import configparser
import copy
import decimal
import hashlib
import json
import os
from pathlib import Path
import random
import statistics
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone

from corpus import CASES, POLICY, validate


HOST = "https://dbc-ca93cffe-fba0.cloud.databricks.com"
WORKSPACE_ID = "3665629714053906"
IDENTITY = "hdude2020@gmail.com"
CLI = r"C:\git\cli\databricks.exe"
CONFIG = r"C:\git\cli\.databrickscfg"
CREDIT_STATE = r"C:\git\cli\.credit-monitor\state.json"
DEFAULT_MODELS = (
    "openjev-qwen35-4b,typesafeai-jev-1-14-0,meta-llama-3-1-8b-instruct,"
    "meta-llama-3-3-70b-instruct,llama-4-maverick,gpt-6-luna,gpt-6-sol,"
    "gpt-6-astra,claude-haiku-5-5,gemini-3-8-flash"
)


def now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def verify_saved_credit_context(config_file=CONFIG, credit_state=CREDIT_STATE):
    settings = configparser.ConfigParser(interpolation=None)
    if not settings.read(config_file, encoding="utf-8-sig"):
        raise ValueError("Databricks CLI configuration is unavailable")
    if settings.get("AWS-DEV", "host", fallback="").rstrip("/") != HOST:
        raise ValueError("CLI profile host does not match AWS-DEV")
    state = json.loads(Path(credit_state).read_text(encoding="utf-8-sig"))
    workspace = state.get("workspaces", {}).get("AWS-DEV", {})
    if str(workspace.get("workspace_id")) != WORKSPACE_ID:
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
    return {"host": HOST, "workspace_id": WORKSPACE_ID, "profile": "AWS-DEV", "checked_at": now(),
            "credit_observed_at": observation.get("observed_at"), "saved_credit_hold": False,
            "saved_remaining_usd": str(balance)}


def verify_manifest_corpus(manifest, require_current=True):
    snapshot = {"policy": manifest["policy"], "cases": manifest["cases"]}
    digest = hashlib.sha256(json.dumps(snapshot, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
    if digest != manifest.get("corpus", {}).get("sha256"):
        raise ValueError("Saved corpus snapshot does not match its manifest hash")
    if require_current and digest != validate()["sha256"]:
        raise ValueError("Current corpus differs from the saved run; refusing to mix benchmark inputs")
    return digest


class ApiError(RuntimeError):
    def __init__(self, status, body, headers):
        self.status = status
        self.body = body
        self.headers = headers
        super().__init__(f"HTTP {status}: {json.dumps(body, ensure_ascii=False)}")


class Client:
    def __init__(self):
        environment = {key: value for key, value in os.environ.items() if not key.startswith("DATABRICKS_")}
        environment["DATABRICKS_CONFIG_FILE"] = CONFIG
        environment["DATABRICKS_CONFIG_PROFILE"] = "AWS-DEV"
        result = subprocess.run(
            [CLI, "auth", "token", "AWS-DEV", "--output", "json"],
            capture_output=True, text=True, encoding="utf-8", env=environment, timeout=90,
        )
        if result.returncode:
            raise RuntimeError("AWS-DEV CLI authentication failed; renew the existing profile.")
        token = json.loads(result.stdout)
        self.token = token["access_token"]

    def call(self, method, path, body=None, tags=None, timeout=180):
        if not path.startswith("/") or path.startswith("//"):
            raise ValueError("An absolute workspace API path is required")
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        if tags:
            headers["Databricks-Ai-Gateway-Request-Tags"] = json.dumps(tags, separators=(",", ":"))
        request = urllib.request.Request(
            HOST + path,
            data=None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8"),
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8")
                return (json.loads(raw) if raw else {}), self.safe_headers(response.headers)
        except urllib.error.HTTPError as error:
            raw = error.read().decode("utf-8", errors="replace")
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = {"message": raw}
            raise ApiError(error.code, payload, self.safe_headers(error.headers)) from None

    @staticmethod
    def safe_headers(headers):
        return {
            key.lower(): value for key, value in headers.items()
            if key.lower() in {"x-request-id", "x-databricks-request-id", "request-id", "date"}
        }

    def verify(self):
        identity, _ = self.call("GET", "/api/2.0/preview/scim/v2/Me")
        if identity.get("userName") != IDENTITY or not identity.get("active"):
            raise RuntimeError("The connected AWS-DEV identity does not match the benchmark owner")
        return {key: identity.get(key) for key in ("id", "userName", "active")}


def question():
    return {
        "moderation": {
            "type": "choice",
            "instructions": POLICY,
            "criteria": {"approved": "The post complies with the policy.", "refused": "The post violates the policy."},
        }
    }


def tag_resource(client, manifest, path, entity_type, name):
    results = []
    for key, value in manifest["tags"].items():
        try:
            response, _ = client.call("POST", "/api/2.1/unity-catalog/entity-tag-assignments", {
                "entity_type": entity_type, "entity_name": name, "tag_key": key, "tag_value": value,
            })
            results.append({"key": key, "status": "created", "response": response})
        except Exception as error:
            results.append({"key": key, "status": "failed", "error": str(error)})
    manifest.setdefault("resource_tags", {})[name] = results
    save(path, manifest)


def create_resource(client, manifest, path, resource, api_path, body):
    resource["creation_requested_at"] = now()
    resource["status"] = "creation_requested"
    save(path, manifest)
    try:
        response, _ = client.call("POST", api_path, body)
    except Exception as error:
        resource["status"] = "creation_uncertain"
        resource["creation_error"] = str(error)
        save(path, manifest)
        raise
    resource.update({"created": True, "status": "created", "creation_response": response})
    resource["id"] = response.get("id") or response.get("model_service_id")
    save(path, manifest)
    return response


def create_catalog_with_default_storage(client, manifest, path, marker):
    catalog = manifest["catalog"]
    endpoint = "/api/2.1/unity-catalog/catalogs/" + catalog["name"]
    prior_error = catalog.get("creation_error", "")
    if catalog.get("creation_requested_at"):
        if "INVALID_STATE" not in prior_error or "Default Storage is enabled" not in prior_error:
            raise RuntimeError("Catalog creation was previously attempted; inspect the manifest before retrying")
        try:
            response, _ = client.call("GET", endpoint)
        except ApiError as error:
            if error.status != 404:
                raise
        else:
            if response.get("comment") != marker:
                raise RuntimeError("An existing catalog has a different ownership marker")
            catalog.update({"created": True, "status": "created", "creation_response": response})
            save(path, manifest)
            return
    else:
        try:
            create_resource(client, manifest, path, catalog, "/api/2.1/unity-catalog/catalogs", {
                "name": catalog["name"], "comment": marker,
            })
            return
        except ApiError as error:
            if error.body.get("error_code") != "INVALID_STATE" or "Default Storage is enabled" not in error.body.get("message", ""):
                raise
    sql_record = catalog.setdefault("sql_creation", {"started_at": now(), "status": "preparing"})
    save(path, manifest)
    if sql_record.get("statement_id"):
        response, _ = client.call("GET", "/api/2.0/sql/statements/" + sql_record["statement_id"])
    else:
        identifier = catalog["name"].replace("`", "``")
        escaped_marker = marker.replace("'", "''")
        tags = {**manifest["tags"], "phase": "setup"}
        response, _ = client.call("POST", "/api/2.0/sql/statements", {
            "warehouse_id": manifest["warehouse_id"],
            "statement": f"CREATE CATALOG `{identifier}` COMMENT '{escaped_marker}'",
            "query_tags": [{"key": key, "value": value} for key, value in tags.items()],
            "wait_timeout": "10s", "on_wait_timeout": "CONTINUE", "disposition": "INLINE", "format": "JSON_ARRAY",
        })
        sql_record["statement_id"] = response["statement_id"]
        manifest.setdefault("statement_ids", []).append(response["statement_id"])
        save(path, manifest)
    deadline = time.monotonic() + 600
    while response["status"]["state"] in {"PENDING", "RUNNING"}:
        if time.monotonic() > deadline:
            client.call("POST", "/api/2.0/sql/statements/" + sql_record["statement_id"] + "/cancel")
            raise TimeoutError("Default-storage catalog creation exceeded ten minutes; cancellation requested")
        time.sleep(3)
        response, _ = client.call("GET", "/api/2.0/sql/statements/" + sql_record["statement_id"])
    sql_record.update({"status": response["status"]["state"], "response": response, "finished_at": now()})
    save(path, manifest)
    if response["status"]["state"] != "SUCCEEDED":
        raise RuntimeError("Default-storage catalog creation failed: " + json.dumps(response["status"]))
    catalog_response, _ = client.call("GET", endpoint)
    if catalog_response.get("comment") != marker:
        raise RuntimeError("Created catalog ownership marker did not match")
    catalog.update({"created": True, "status": "created", "creation_response": catalog_response})
    save(path, manifest)


def setup(client, manifest, path, inventory):
    if manifest.get("setup_complete"):
        return
    if manifest.get("schema", {}).get("creation_requested_at") or manifest.get("model_services"):
        raise RuntimeError("Setup was previously attempted; review the saved manifest and cleanup before creating another run")
    catalog = manifest["catalog"]
    marker = f"Temporary moderation benchmark {manifest['run_id']}"
    create_catalog_with_default_storage(client, manifest, path, marker)
    tag_resource(client, manifest, path, "catalogs", catalog["name"])
    schema = manifest["schema"]
    create_resource(client, manifest, path, schema, "/api/2.1/unity-catalog/schemas", {
        "catalog_name": catalog["name"], "name": "moderation", "comment": marker,
    })
    tag_resource(client, manifest, path, "schemas", schema["name"])
    model_map = {item["name"].removeprefix("model-services/"): item for item in inventory}
    for model in manifest["requested_models"]:
        source = model_map.get(model)
        if source is None:
            manifest["skipped_models"].append({"model": model, "reason": "Not present in the verified system.ai inventory"})
            save(path, manifest)
            continue
        destinations = source.get("config", {}).get("routing", {}).get("destinations", [])
        if not destinations or any(item.get("destination_type") != "DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL" for item in destinations):
            manifest["skipped_models"].append({"model": model, "reason": "Only pay-per-token foundation-model destinations are enabled"})
            save(path, manifest)
            continue
        routing = {"destinations": [
            {key: copy.deepcopy(value) for key, value in item.items() if key in {"name", "destination_type", "pay_per_token_config", "traffic_percentage"}}
            for item in destinations
        ]}
        leaf = model.removeprefix("system.ai.").replace("-", "_")
        record = {"name": f"{schema['name']}.{leaf}", "model": model, "created": False, "supported_api_types": source.get("supported_api_types", [])}
        manifest["model_services"].append(record)
        save(path, manifest)
        query = urllib.parse.urlencode({"parent": f"schemas/{schema['name']}", "model_service_id": leaf})
        try:
            response = create_resource(client, manifest, path, record,
                "/api/2.1/unity-catalog/model-services?" + query,
                {"comment": marker, "config": {"routing": routing}},
            )
            record["supported_api_types"] = response.get("supported_api_types", record["supported_api_types"])
            tag_resource(client, manifest, path, "model-services", record["name"])
        except ApiError as error:
            record["status"] = "creation_failed"
            record["creation_error"] = str(error)
            save(path, manifest)
    manifest["setup_complete"] = True
    manifest["setup_finished_at"] = now()
    save(path, manifest)


def build_request(model, text):
    if model["model"] == "ai_decide_rest":
        return "/api/2.0/ai-functions/ai-decide", {"state": text, "questions": question(), "options": {"version": "1.0"}}, "decision"
    service = model["name"]
    supported = model.get("supported_api_types", [])
    if "typesafe/v1/systemone" in supported:
        return "/ai-gateway/typesafe/v1/systemone", {"model": service, "state": text, "questions": question()}, "systemone"
    messages = [
        {"role": "system", "content": POLICY + " Return exactly one lowercase word: approved or refused."},
        {"role": "user", "content": text},
    ]
    if model["model"].startswith("system.ai.gpt-6") and "openai/v1/responses" in supported:
        return "/ai-gateway/openai/v1/responses", {
            "model": service, "input": messages, "max_output_tokens": 256, "reasoning": {"effort": "low"},
        }, "responses"
    if "mlflow/v1/chat/completions" not in supported:
        raise ValueError("No supported benchmark inference API for this service")
    body = {"model": service, "messages": messages, "max_tokens": 16}
    if model["model"] == "system.ai.gemini-3-8-flash":
        body.update({"max_tokens": 256, "reasoning_effort": "low"})
    if not model["model"].startswith("system.ai.gemini-") and model["model"] != "system.ai.claude-haiku-5-5":
        body["temperature"] = 0
    return "/ai-gateway/mlflow/v1/chat/completions", body, "chat"


def parse_prediction(response, kind):
    if kind == "decision":
        text = response.get("response", {}).get("answers", {}).get("moderation", {}).get("choice")
    elif kind == "systemone":
        text = response.get("answers", {}).get("moderation", {}).get("choice")
    elif kind == "responses":
        text = "".join(
            part.get("text", "")
            for item in response.get("output", []) if item.get("type") == "message"
            for part in item.get("content", []) if part.get("type") == "output_text"
        )
    else:
        choices = response.get("choices", [])
        text = choices[0].get("message", {}).get("content") if choices else None
        if isinstance(text, list):
            text = "".join(part["text"] for part in text if isinstance(part, dict) and part.get("type") in {"text", "output_text"} and isinstance(part.get("text"), str))
    label = text.strip() if isinstance(text, str) else None
    return label if label in {"approved", "refused"} else None, text


def prepare_rejected_retries(manifest, path):
    retained = []
    rejected = []
    expected_messages = {
        "system.ai.gemini-3-8-flash": "BAD_REQUEST: Model gemini-3.8-flash does not support the temperature parameter.",
        "system.ai.claude-haiku-5-5": "BAD_REQUEST: Model us.anthropic.claude-haiku-5-5 does not support the temperature parameter.",
    }
    for record in manifest.get("requests", []):
        response = record.get("response")
        matched = (
            record.get("model") in expected_messages
            and record.get("status") == "error"
            and record.get("http_status") == 400
            and record.get("endpoint") == "/ai-gateway/mlflow/v1/chat/completions"
            and "temperature" in record.get("request_body", {})
            and isinstance(response, dict)
            and response.get("error_code") == "BAD_REQUEST"
            and response.get("message") == expected_messages.get(record.get("model"))
        )
        if matched:
            rejected.append({**record, "original_status": record["status"], "status": "rejected_before_inference", "retry_authorized_at": now()})
        else:
            retained.append(record)
    if rejected:
        manifest["requests"] = retained
        manifest.setdefault("rejected_requests", []).extend(rejected)
        save(path, manifest)
    print(json.dumps({"rejected_requests_prepared_for_retry": len(rejected)}), flush=True)
    return len(rejected)


def prepare_truncated_retries(manifest, path):
    retained = []
    calibration = []
    for record in manifest.get("requests", []):
        response = record.get("response")
        choices = response.get("choices", []) if isinstance(response, dict) else []
        matched = (
            record.get("model") == "system.ai.gemini-3-8-flash"
            and record.get("status") in {"ok", "invalid_output"}
            and record.get("endpoint") == "/ai-gateway/mlflow/v1/chat/completions"
            and record.get("request_body", {}).get("max_tokens") == 16
            and len(choices) == 1
            and choices[0].get("finish_reason") == "length"
        )
        if matched:
            calibration.append({
                **record, "original_status": record["status"], "status": "calibration_truncated",
                "superseded_config": True, "retry_authorized_at": now(),
                "replacement_parameters": {"max_tokens": 256, "reasoning_effort": "low"},
            })
        else:
            retained.append(record)
    if calibration:
        manifest["requests"] = retained
        manifest.setdefault("calibration_requests", []).extend(calibration)
        save(path, manifest)
    print(json.dumps({"truncated_calibration_requests_prepared_for_retry": len(calibration)}), flush=True)
    return len(calibration)


def run(client, manifest, path):
    verify_manifest_corpus(manifest)
    if not manifest.get("setup_complete"):
        raise RuntimeError("Setup must complete before inference")
    if manifest.get("cleanup_complete"):
        raise RuntimeError("These temporary services have already been removed")
    models = [{"model": "ai_decide_rest", "name": "ai_decide_rest"}]
    for item in manifest["model_services"]:
        if not item.get("created"):
            continue
        assignments = manifest.get("resource_tags", {}).get(item["name"], [])
        applied = {entry["key"] for entry in assignments if entry.get("status") == "created"}
        if not set(manifest["tags"]).issubset(applied):
            item["inference_skipped"] = "Required model-service cost attribution tags were not applied"
            continue
        models.append(item)
    tasks = [(model, case, repeat) for repeat in range(manifest["repeats"]) for model in models for case in CASES]
    random.Random(manifest["seed"]).shuffle(tasks)
    attempted = {(item["model"], item["case_id"], item["repeat"]) for item in manifest.get("requests", [])}
    manifest.setdefault("inference_started_at", now())
    for model, case, repeat in tasks:
        key = (model["model"], case["id"], repeat)
        if key in attempted:
            continue
        request_id = str(uuid.uuid4())
        tags = {**manifest["tags"], "model": model["model"], "case_id": case["id"], "phase": "measured", "repeat": str(repeat), "client_request_id": request_id}
        record = {
            "client_request_id": request_id, "model": model["model"], "service": model["name"],
            "case_id": case["id"], "repeat": repeat, "expected": "approved" if case["expected_approved"] else "refused",
            "started_at": now(), "status": "submitted", "tags": tags,
        }
        manifest.setdefault("requests", []).append(record)
        if model["model"] == "ai_decide_rest":
            manifest["ai_decide"].setdefault("started_at", record["started_at"])
        save(path, manifest)
        start = time.perf_counter()
        try:
            endpoint, body, kind = build_request(model, case["text"])
            record.update({"endpoint": endpoint, "request_body": body, "api_kind": kind})
            response, headers = client.call("POST", endpoint, body, tags=tags)
            record["client_latency_ms"] = (time.perf_counter() - start) * 1000
            prediction, output_text = parse_prediction(response, kind)
            record.update({
                "response": response, "response_headers": headers, "prediction": prediction,
                "output_text": output_text, "correct": prediction == record["expected"],
                "status": "ok" if prediction is not None else "invalid_output", "usage": response.get("usage"),
            })
        except Exception as error:
            record["client_latency_ms"] = (time.perf_counter() - start) * 1000
            record.update({"status": "error", "error": str(error), "correct": False})
            if isinstance(error, ApiError):
                record.update({"http_status": error.status, "response": error.body, "response_headers": error.headers})
        record["finished_at"] = now()
        if model["model"] == "ai_decide_rest":
            manifest["ai_decide"]["finished_at"] = record["finished_at"]
        save(path, manifest)
        print(json.dumps({key: record.get(key) for key in ("model", "case_id", "status", "prediction", "correct", "client_latency_ms")}), flush=True)
    manifest["inference_finished_at"] = now()
    manifest["finished_at"] = manifest["inference_finished_at"]
    manifest["summary"] = summarize(manifest)
    save(path, manifest)
    save(path.parent / "summary.json", manifest["summary"])


def token_totals(rows):
    token_values = {key: [] for key in ("input_tokens", "output_tokens", "reasoning_tokens", "total_tokens", "cached_input_tokens")}
    for row in rows:
        usage = row.get("usage")
        usage = usage if isinstance(usage, dict) else {}
        token_values["input_tokens"].append(usage.get("input_tokens", usage.get("prompt_tokens")))
        token_values["output_tokens"].append(usage.get("output_tokens", usage.get("completion_tokens")))
        details = usage.get("output_tokens_details") or usage.get("completion_tokens_details") or {}
        token_values["reasoning_tokens"].append(usage.get("reasoning_tokens", details.get("reasoning_tokens")))
        token_values["total_tokens"].append(usage.get("total_tokens"))
        input_details = usage.get("input_tokens_details") or usage.get("prompt_tokens_details") or {}
        token_values["cached_input_tokens"].append(usage.get("cached_input_tokens", usage.get("cache_read_input_tokens", input_details.get("cached_tokens"))))
    return {
        key: sum(values) if rows and all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in values) else None
        for key, values in token_values.items()
    }


def summarize(manifest):
    verify_manifest_corpus(manifest, require_current=False)
    summary = []
    for model in dict.fromkeys(item["model"] for item in manifest.get("requests", [])):
        rows = [item for item in manifest["requests"] if item["model"] == model]
        timings = [item["client_latency_ms"] for item in rows if item["status"] in {"ok", "invalid_output"}]
        calibration = [item for item in manifest.get("calibration_requests", []) if item["model"] == model]
        semantics = "provider_reported_output"
        if model == "system.ai.gemini-3-8-flash":
            semantics = "completion_tokens_excludes_reasoning_tokens"
        elif model.startswith("system.ai.gpt-6"):
            semantics = "output_tokens_includes_reasoning_tokens"
        per_case = []
        for case in manifest["cases"]:
            case_rows = sorted((item for item in rows if item["case_id"] == case["id"]), key=lambda item: item["repeat"])
            case_timings = [item["client_latency_ms"] for item in case_rows if item["status"] in {"ok", "invalid_output"}]
            predictions = [item.get("prediction") for item in case_rows]
            expected_repeats = set(range(manifest["repeats"]))
            repeats_complete = len(case_rows) == len(expected_repeats) and {item["repeat"] for item in case_rows} == expected_repeats
            stability_available = repeats_complete and len(expected_repeats) >= 2
            per_case.append({
                "case_id": case["id"], "attempts": len(case_rows),
                "predictions_by_repeat": [{"repeat": item["repeat"], "prediction": item.get("prediction"), "status": item["status"], "correct": item.get("correct", False)} for item in case_rows],
                "stability_status": "complete" if stability_available else "incomplete" if not repeats_complete else "single_repeat",
                "valid_decision_consistent": all(item["status"] == "ok" for item in case_rows) and len(set(predictions)) == 1 if stability_available else None,
                "correct": sum(bool(item.get("correct")) for item in case_rows),
                "latency_ms": {"median": statistics.median(case_timings), "min": min(case_timings), "max": max(case_timings)} if case_timings else None,
            })
        summary.append({
            "model": model, "attempts": len(rows), "unique_posts": len({item["case_id"] for item in rows}),
            "rejected_before_inference": sum(item["model"] == model for item in manifest.get("rejected_requests", [])),
            "calibration_attempts": len(calibration), "calibration_tokens": token_totals(calibration),
            "correct": sum(bool(item.get("correct")) for item in rows),
            "errors": sum(item["status"] == "error" for item in rows),
            "invalid_outputs": sum(item["status"] == "invalid_output" for item in rows),
            "uncertain_submissions": sum(item["status"] == "submitted" for item in rows),
            "latency_ms": {"median": statistics.median(timings), "min": min(timings), "max": max(timings)} if timings else None,
            "latency_samples": len(timings), "per_case": per_case,
            **token_totals(rows), "output_token_semantics": semantics,
            "billing_status": "pending_reconciliation",
        })
    return summary


def cleanup_default_schema(client, manifest, path):
    catalog = manifest["catalog"]
    if not catalog.get("created"):
        raise RuntimeError("The parent catalog was not confirmed created by this run")
    name = catalog["name"] + ".default"
    endpoint = "/api/2.1/unity-catalog/schemas/" + name
    record = manifest.setdefault("default_schema", {"name": name, "source": "catalog_auto_created"})
    try:
        existing, _ = client.call("GET", endpoint)
    except ApiError as error:
        if error.status != 404:
            raise
        record["cleanup_status"] = "verified_absent"
        save(path, manifest)
        return
    expected = {
        "name": "default", "full_name": name, "catalog_name": catalog["name"],
        "comment": "Default schema (auto-created)", "created_by": IDENTITY,
    }
    if any(existing.get(key) != value for key, value in expected.items()):
        raise RuntimeError("Auto-created default schema identity does not match; deletion skipped")
    record["verified_metadata"] = existing
    record["deletion_requested_at"] = now()
    save(path, manifest)
    client.call("DELETE", endpoint)
    try:
        client.call("GET", endpoint)
    except ApiError as error:
        if error.status != 404:
            raise
    else:
        raise RuntimeError("Auto-created default schema still exists after non-force deletion")
    record["cleanup_status"] = "verified_absent"
    record["deleted_at"] = now()
    save(path, manifest)


def cleanup(client, manifest, path):
    errors = []
    resources = [(item, "/api/2.1/unity-catalog/model-services/" + item["name"]) for item in reversed(manifest["model_services"])]
    resources += [(manifest["schema"], "/api/2.1/unity-catalog/schemas/" + manifest["schema"]["name"])]
    resources += [(manifest["catalog"], "/api/2.1/unity-catalog/catalogs/" + manifest["catalog"]["name"])]
    marker = f"Temporary moderation benchmark {manifest['run_id']}"
    for resource, endpoint in resources:
        if not resource.get("creation_requested_at"):
            continue
        try:
            try:
                existing, _ = client.call("GET", endpoint)
            except ApiError as error:
                if error.status == 404:
                    resource["cleanup_status"] = "verified_absent"
                    save(path, manifest)
                    continue
                raise
            if existing.get("comment") != marker:
                raise RuntimeError("Resource ownership marker does not match this run; deletion skipped")
            if resource is manifest["catalog"]:
                cleanup_default_schema(client, manifest, path)
            client.call("DELETE", endpoint)
            try:
                client.call("GET", endpoint)
            except ApiError as error:
                if error.status != 404:
                    raise
            else:
                raise RuntimeError("Resource still exists after deletion")
            resource["cleanup_status"] = "verified_absent"
            resource["deleted_at"] = now()
        except Exception as error:
            resource["cleanup_status"] = "failed"
            resource["cleanup_error"] = str(error)
            errors.append({"name": resource["name"], "error": str(error)})
        save(path, manifest)
    manifest["cleanup_errors"] = errors
    manifest["cleanup_complete"] = not errors
    manifest["cleanup_checked_at"] = now()
    save(path, manifest)
    print(json.dumps({"cleanup_complete": not errors, "errors": errors}), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--inventory", type=Path)
    parser.add_argument("--models", default=DEFAULT_MODELS)
    parser.add_argument("--repeats", type=int, default=1, choices=range(1, 4))
    parser.add_argument("--seed", type=int, default=20261027)
    parser.add_argument("--phase", choices=("setup", "run", "cleanup", "all"), default="all")
    parser.add_argument("--retry-rejected", action="store_true")
    parser.add_argument("--retry-truncated", action="store_true")
    args = parser.parse_args()
    if (args.retry_rejected or args.retry_truncated) and args.phase != "run":
        parser.error("Retry flags are only valid with --phase run")
    output = args.output_dir.resolve()
    if output.is_relative_to(Path(__file__).resolve().parents[3]):
        raise ValueError("Save benchmark output outside the source repository")
    output.mkdir(parents=True, exist_ok=True)
    path = output / "manifest.json"
    if path.exists():
        manifest = json.loads(path.read_text(encoding="utf-8-sig"))
        if manifest.get("host") != HOST or manifest.get("workspace_id") != WORKSPACE_ID:
            raise ValueError("Manifest does not belong to AWS-DEV")
    else:
        if args.phase not in {"setup", "all"}:
            raise ValueError("No existing manifest for this phase")
        identifier = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
        run_id = "ai_decide_" + identifier
        catalog = "ai_decide_bench_" + identifier
        manifest = {
            "run_id": run_id, "started_at": now(), "host": HOST, "workspace_id": WORKSPACE_ID,
            "profile": "AWS-DEV", "warehouse_id": "8a19c545d43c9bc3", "repeats": args.repeats, "seed": args.seed,
            "tags": {"run_id": run_id, "project": "ai_decide_moderation", "environment": "AWS-DEV"},
            "catalog": {"name": catalog, "created": False}, "schema": {"name": catalog + ".moderation", "created": False},
            "requested_models": list(dict.fromkeys("system.ai." + item.removeprefix("system.ai.") for item in args.models.split(",") if item)),
            "model_services": [], "skipped_models": [], "ai_decide": {"statement_ids": [], "request_tag_propagation": "unverified"},
            "statement_ids": [], "requests": [], "policy": POLICY, "cases": CASES, "corpus": validate(),
            "cost_status": "pending_reconciliation", "sampling_note": "Five synthetic English posts; illustrative comparison, not a population accuracy estimate.",
        }
        save(path, manifest)
    if args.phase in {"setup", "run", "all"}:
        verify_manifest_corpus(manifest)
        manifest["credit_preflight"] = verify_saved_credit_context()
        save(path, manifest)
    client = Client()
    manifest["verified_identity"] = client.verify()
    save(path, manifest)
    inventory = None
    if args.phase in {"setup", "all"}:
        if args.inventory is None:
            raise ValueError("--inventory is required for setup")
        raw = args.inventory.read_bytes()
        inventory = json.loads(raw.decode("utf-8-sig"))
        manifest["inventory_sha256"] = hashlib.sha256(raw).hexdigest()
        save(path, manifest)
    if args.phase == "all":
        try:
            setup(client, manifest, path, inventory)
            run(client, manifest, path)
        finally:
            cleanup(client, manifest, path)
    elif args.phase == "setup":
        setup(client, manifest, path, inventory)
    elif args.phase == "run":
        if args.retry_rejected:
            prepare_rejected_retries(manifest, path)
        if args.retry_truncated:
            prepare_truncated_retries(manifest, path)
        run(client, manifest, path)
    else:
        cleanup(client, manifest, path)
    if manifest.get("cleanup_errors"):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
