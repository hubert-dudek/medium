import argparse
import json
import os
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from corpus import CASES, POLICY, validate
from benchmark import HOST, WORKSPACE_ID, question, verify_manifest_corpus, verify_saved_credit_context


CLI = r"C:\git\cli\databricks.exe"
WAREHOUSE = "8a19c545d43c9bc3"


def now():
    return datetime.now(timezone.utc).isoformat()


def api(method, path, body=None):
    env = {key: value for key, value in os.environ.items() if not key.startswith("DATABRICKS_")}
    env["DATABRICKS_CONFIG_FILE"] = r"C:\git\cli\.databrickscfg"
    command = [CLI, "api", method, path, "--profile", "AWS-DEV", "--output", "json"]
    temporary = None
    try:
        if body is not None:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".json", delete=False) as handle:
                json.dump(body, handle, ensure_ascii=False)
                temporary = handle.name
            command.extend(["--json", "@" + temporary])
        result = subprocess.run(command, env=env, capture_output=True, text=True, encoding="utf-8", timeout=120)
        if result.returncode or result.stderr.strip().startswith("Error:"):
            raise RuntimeError(result.stderr.strip())
        return json.loads(result.stdout) if result.stdout.strip() else {}
    finally:
        if temporary:
            Path(temporary).unlink(missing_ok=True)


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args()
    manifest_path = args.manifest.resolve()
    repo = Path(__file__).resolve().parents[3]
    if manifest_path.is_relative_to(repo):
        raise ValueError("Results must be outside the code repository")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("host") != HOST or str(manifest.get("workspace_id")) != WORKSPACE_ID:
        raise ValueError("Manifest does not belong to AWS-DEV")
    prior_sql = manifest.get("ai_decide_sql", {})
    if prior_sql.get("statement_id") or prior_sql.get("submission_started_at"):
        raise ValueError("SQL submission was already attempted; reconcile query history before retrying")
    verify_manifest_corpus(manifest)
    verify_saved_credit_context()
    identity = api("get", "/api/2.0/preview/scim/v2/Me")
    if identity.get("userName") != "hdude2020@gmail.com":
        raise ValueError("Unexpected AWS-DEV identity")
    questions = question()
    sql = "WITH posts AS (SELECT * FROM VALUES\n" + ",\n".join(
        f"(:id_{i}, :text_{i}, :expected_{i})" for i in range(len(CASES))
    ) + "\nAS p(case_id, post_text, expected_label))\nSELECT case_id, expected_label, ai_decide(post_text, :questions, map('version', '1.0')) AS decision FROM posts"
    parameters = [{"name": "questions", "value": json.dumps(questions)}]
    for index, case in enumerate(CASES):
        for key, value in {"id": case["id"], "text": case["text"], "expected": "approved" if case["expected_approved"] else "refused"}.items():
            parameters.append({"name": f"{key}_{index}", "value": value})
    tags = {"project": "ai_decide_moderation", "run_id": manifest["run_id"], "phase": "sql_benchmark", "model": "ai_decide_sql"}
    before = api("get", f"/api/2.0/sql/warehouses/{WAREHOUSE}")
    warehouse_tags = before.get("tags", {}).get("custom_tags", [])
    old_values = {tag["key"]: tag["value"] for tag in warehouse_tags}
    if any(key in old_values and old_values[key] != value for key, value in tags.items()):
        raise ValueError("Warehouse benchmark tag collision")
    added = {key: value for key, value in tags.items() if key not in old_values}
    merged = dict(old_values, **added)
    record = {"started_at": now(), "corpus": validate(), "warehouse_before": before, "query_tags": tags,
              "warehouse_tags_added": added, "status": "preparing"}
    manifest["ai_decide_sql"] = record
    save(manifest_path, manifest)
    save(manifest_path.with_name("sql_request.json"), {"statement": sql, "parameters": parameters})
    try:
        record["warehouse_tags_requested"] = True
        save(manifest_path, manifest)
        api("post", f"/api/2.0/sql/warehouses/{WAREHOUSE}/edit", {"tags": {"custom_tags": [{"key": k, "value": v} for k, v in merged.items()]}})
        record["warehouse_tags_applied"] = True
        save(manifest_path, manifest)
        started = time.perf_counter()
        record["submission_started_at"] = now()
        save(manifest_path, manifest)
        response = api("post", "/api/2.0/sql/statements", {"warehouse_id": WAREHOUSE, "statement": sql,
            "parameters": parameters, "query_tags": [{"key": k, "value": v} for k, v in tags.items()],
            "wait_timeout": "10s", "on_wait_timeout": "CONTINUE", "disposition": "INLINE", "format": "JSON_ARRAY"})
        record["statement_id"] = response["statement_id"]
        manifest.setdefault("statement_ids", []).append(response["statement_id"])
        manifest.setdefault("ai_decide", {}).setdefault("statement_ids", []).append(response["statement_id"])
        save(manifest_path, manifest)
        while response["status"]["state"] in {"PENDING", "RUNNING"}:
            if time.perf_counter() - started > 600:
                api("post", f"/api/2.0/sql/statements/{response['statement_id']}/cancel")
                raise TimeoutError("SQL benchmark exceeded 10 minutes; cancellation requested")
            time.sleep(3)
            response = api("get", f"/api/2.0/sql/statements/{response['statement_id']}")
        record["elapsed_seconds_including_startup"] = time.perf_counter() - started
        record["status"] = response["status"]["state"]
        save(manifest_path.with_name("sql_response.json"), response)
        if response["status"]["state"] != "SUCCEEDED":
            record["error"] = response["status"].get("error")
        else:
            rows = []
            for case_id, expected, raw in response.get("result", {}).get("data_array", []):
                decision = json.loads(raw) if isinstance(raw, str) else raw
                label = decision.get("response", {}).get("answers", {}).get("moderation", {}).get("choice")
                rows.append({"case_id": case_id, "expected": expected, "label": label, "correct": label == expected, "raw": decision})
            record["rows"] = rows
            record["correct"] = sum(row["correct"] for row in rows)
            record["count"] = len(rows)
            if len(rows) != len(CASES):
                record["status"] = "incomplete_result"
    finally:
        record["finished_at"] = now()
        manifest["finished_at"] = now()
        if record.get("warehouse_tags_requested"):
            try:
                current = api("get", f"/api/2.0/sql/warehouses/{WAREHOUSE}")
                retained = [tag for tag in current.get("tags", {}).get("custom_tags", []) if added.get(tag["key"]) != tag["value"]]
                api("post", f"/api/2.0/sql/warehouses/{WAREHOUSE}/edit", {"tags": {"custom_tags": retained}})
                after = api("get", f"/api/2.0/sql/warehouses/{WAREHOUSE}")
                record["warehouse_tags_restored"] = after.get("tags", {}).get("custom_tags", []) == retained
            except Exception as error:
                record["restore_error"] = str(error)
        save(manifest_path, manifest)
    print(json.dumps({key: record.get(key) for key in ["status", "statement_id", "correct", "count", "elapsed_seconds_including_startup", "error", "warehouse_tags_restored"]}, indent=2))


if __name__ == "__main__":
    main()
