import argparse
import base64
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlencode

from benchmark import HOST, WORKSPACE_ID, now, save, verify_manifest_corpus, verify_saved_credit_context
from corpus import CASES, POLICY, validate
from run_sql import api


def write(manifest_path, manifest):
    save(manifest_path, manifest)


def start(manifest_path):
    if manifest_path.exists():
        raise ValueError("Manifest already exists; use status instead of submitting another run")
    credit = verify_saved_credit_context()
    identity = api("get", "/api/2.0/preview/scim/v2/Me")
    if identity.get("userName") != "hdude2020@gmail.com" or identity.get("active") is not True:
        raise ValueError("Unexpected AWS-DEV identity")
    run_id = "ai_decide_serverless_" + now()[:19].replace("-", "").replace(":", "").replace("T", "_") + "_" + uuid.uuid4().hex[:6]
    directory = "/Workspace/Users/hdude2020@gmail.com/" + run_id
    tags = {"run_id": run_id, "project": "ai_decide_moderation", "environment": "AWS-DEV", "phase": "serverless_benchmark"}
    manifest = {
        "run_id": run_id, "host": HOST, "workspace_id": WORKSPACE_ID,
        "started_at": now(), "policy": POLICY, "cases": CASES, "corpus": validate(),
        "credit_check": credit, "workspace_directory": directory,
        "job_tags": tags, "serverless_billing_tag_propagation": "unverified_use_job_and_run_ids",
        "performance_target": "STANDARD", "environment_version": "6",
        "serverless_job": {}, "status": "preparing", "model_services": [],
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    write(manifest_path, manifest)
    api("post", "/api/2.0/workspace/mkdirs", {"path": directory})
    manifest["directory_created"] = True
    write(manifest_path, manifest)
    for filename in ("corpus.py", "benchmark.py", "serverless_notebook.py"):
        path = Path(__file__).with_name(filename)
        payload = {"path": directory + "/" + filename, "content": base64.b64encode(path.read_bytes()).decode("ascii"), "overwrite": False}
        payload.update({"format": "SOURCE", "language": "PYTHON"} if filename == "serverless_notebook.py" else {"format": "AUTO"})
        api("post", "/api/2.0/workspace/import", payload)
        obj = api("get", "/api/2.0/workspace/get-status?path=" + quote(payload["path"], safe=""))
        expected = "NOTEBOOK" if filename == "serverless_notebook.py" else "FILE"
        if obj.get("object_type") != expected:
            raise ValueError("Uploaded source has unexpected workspace object type")
        manifest.setdefault("workspace_objects", []).append(obj)
        write(manifest_path, manifest)
    settings = {
        "name": run_id, "tags": tags, "performance_target": "STANDARD",
        "max_concurrent_runs": 1, "timeout_seconds": 900,
        "tasks": [{
            "task_key": "moderation", "environment_key": "benchmark",
            "max_retries": 0, "retry_on_timeout": False, "disable_auto_optimization": True,
            "timeout_seconds": 900,
            "notebook_task": {"notebook_path": directory + "/serverless_notebook.py", "source": "WORKSPACE", "base_parameters": {
                "source_dir": directory, "run_id": run_id,
                "job_id": "{{job.id}}", "job_run_id": "{{job.run_id}}", "task_run_id": "{{task.run_id}}",
            }},
        }],
        "environments": [{"environment_key": "benchmark", "spec": {"environment_version": "6"}}],
    }
    save(manifest_path.with_name("job_request.json"), settings)
    manifest["job_creation_requested_at"] = now()
    write(manifest_path, manifest)
    created = api("post", "/api/2.2/jobs/create", settings)
    job_id = created["job_id"]
    manifest["serverless_job"]["job_id"] = job_id
    write(manifest_path, manifest)
    saved_job = api("get", "/api/2.2/jobs/get?job_id=" + str(job_id))
    save(manifest_path.with_name("job_verified.json"), saved_job)
    actual = saved_job["settings"]
    task = actual["tasks"][0]
    if actual.get("tags") != tags or actual.get("performance_target") != "STANDARD":
        raise ValueError("Saved job tags or performance mode differ")
    if task.get("max_retries", 0) != 0 or task.get("retry_on_timeout", False) or task.get("disable_auto_optimization") is not True:
        raise ValueError("Saved job retry policy differs")
    if any(task.get(k) for k in ("existing_cluster_id", "new_cluster", "job_cluster_key")):
        raise ValueError("Job is not configured for serverless compute")
    manifest["submission_started_at"] = now()
    manifest["idempotency_token"] = str(uuid.uuid4())
    write(manifest_path, manifest)
    submitted = api("post", "/api/2.2/jobs/run-now", {"job_id": job_id, "idempotency_token": manifest["idempotency_token"]})
    manifest["serverless_job"]["run_id"] = submitted["run_id"]
    manifest["status"] = "submitted"
    write(manifest_path, manifest)
    return manifest


def status(manifest_path):
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("host") != HOST or str(manifest.get("workspace_id")) != WORKSPACE_ID:
        raise ValueError("Manifest is not AWS-DEV")
    verify_manifest_corpus(manifest)
    run_id = manifest["serverless_job"].get("run_id")
    if not run_id:
        raise ValueError("No saved job run ID; reconcile the saved submission before any retry")
    details = api("get", "/api/2.2/jobs/runs/get?run_id=" + str(run_id))
    save(manifest_path.with_name("job_run.json"), details)
    manifest["last_checked_at"] = now()
    manifest["serverless_job"]["state"] = details.get("state")
    manifest["serverless_job"]["run_page_url"] = details.get("run_page_url")
    tasks = details.get("tasks", [])
    if tasks:
        manifest["serverless_job"]["task_run_id"] = tasks[0]["run_id"]
    if details.get("state", {}).get("life_cycle_state") in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
        end_millis = details.get("end_time")
        manifest["finished_at"] = datetime.fromtimestamp(end_millis / 1000, timezone.utc).isoformat() if end_millis else manifest.get("finished_at", now())
        manifest["status"] = details.get("state", {}).get("result_state", "unknown")
        if tasks:
            output = api("get", "/api/2.2/jobs/runs/get-output?run_id=" + str(tasks[0]["run_id"]))
            save(manifest_path.with_name("job_output.json"), output)
            result = output.get("notebook_output", {}).get("result")
            if result:
                parsed = json.loads(result)
                if parsed.get("run_id") != manifest["run_id"] or parsed.get("corpus", {}).get("sha256") != manifest["corpus"]["sha256"]:
                    raise ValueError("Notebook output does not match saved run")
                manifest["result"] = parsed
                manifest["status"] = parsed.get("status", "unknown")
    write(manifest_path, manifest)
    return manifest


def history(manifest_path):
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("host") != HOST or str(manifest.get("workspace_id")) != WORKSPACE_ID:
        raise ValueError("Manifest is not AWS-DEV")
    job = manifest["serverless_job"]
    run = json.loads(manifest_path.with_name("job_run.json").read_text(encoding="utf-8"))
    parameters = {
        "filter_by.query_start_time_range.start_time_ms": run["start_time"],
        "filter_by.query_start_time_range.end_time_ms": run.get("end_time") or int(datetime.now(timezone.utc).timestamp() * 1000),
        "filter_by.user_ids": "477935517597058", "max_results": 100, "include_metrics": "true",
    }
    matches = []
    matched_ids = set()
    more = False
    for _ in range(3):
        response = api("get", "/api/2.0/sql/history/queries?" + urlencode(parameters))
        for query in response.get("res", []):
            info = (query.get("query_source") or {}).get("job_info") or {}
            text = query.get("query_text") or ""
            same_job = job.get("job_id") is not None and str(info.get("job_id")) == str(job["job_id"])
            same_run = job.get("run_id") is not None and str(info.get("job_run_id")) == str(job["run_id"])
            same_task = job.get("task_run_id") is not None and str(info.get("job_task_run_id")) == str(job["task_run_id"])
            by_job = same_job and (same_run or same_task)
            by_marker = not info.get("job_id") and "run_id=" + manifest["run_id"] + ";" in text and "ai_decide(" in text.lower()
            query_id = query.get("query_id")
            is_select = str(query.get("statement_type") or "").upper() == "SELECT"
            if is_select and (by_job or by_marker) and (not query_id or query_id not in matched_ids):
                matches.append(query)
                if query_id:
                    matched_ids.add(query_id)
        more = response.get("has_next_page", False)
        if not more:
            break
        token = response.get("next_page_token")
        if not token:
            break
        parameters["page_token"] = token
    record = {"checked_at": now(), "new_sql_queries_executed": False, "matches": matches, "pages_truncated": more}
    save(manifest_path.with_name("query_history.json"), record)
    result = dict(manifest.get("query_history") or {})
    check_status = "truncated" if more else "not_found" if not matches else "ambiguous" if len(matches) > 1 else "matched"
    result.update({"checked_at": record["checked_at"], "matching_record_count": len(matches),
                   "pages_truncated": more, "check_status": check_status,
                   "execution_metric_verified_this_check": False})
    if len(matches) == 1 and not more:
        query = matches[0]
        metrics = query.get("metrics", {})
        current = {"statement_id": query.get("query_id"), "status": query.get("status"),
                   "is_final": query.get("is_final"), "metrics": metrics}
        result["latest_match"] = current
        duration = metrics.get("execution_time_ms")
        valid_duration = isinstance(duration, (int, float)) and not isinstance(duration, bool) and 0 <= duration < float("inf")
        previous_statement = result.get("statement_id") if "execution_seconds" in result else None
        if previous_statement and previous_statement != query.get("query_id"):
            result["check_status"] = "ambiguous"
        elif query.get("status") == "FINISHED" and query.get("is_final") is True and valid_duration:
            result.update(current)
            result.update({"execution_seconds": duration / 1000,
                           "execution_metric_verified_at": record["checked_at"],
                           "execution_metric_verified_this_check": True})
        else:
            result["check_status"] = "metrics_pending"
            if "execution_seconds" not in result:
                result.update(current)
    result["execution_metric_retained"] = "execution_seconds" in result and not result["execution_metric_verified_this_check"]
    manifest["query_history"] = result
    write(manifest_path, manifest)
    return manifest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("start", "status", "history"))
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    target = args.manifest.resolve()
    if target.is_relative_to(Path(__file__).resolve().parents[3]):
        raise ValueError("Private results must be outside the Git repository")
    if args.action == "start" and not args.execute:
        raise ValueError("Starting the five-post job requires --execute")
    manifest = {"start": start, "status": status, "history": history}[args.action](target)
    result = manifest.get("result", {})
    print(json.dumps({"manifest": str(target), "status": manifest["status"], "serverless_job": manifest["serverless_job"],
                      "query_history": manifest.get("query_history"),
                      "result": {k: result.get(k) for k in ("count", "correct", "invalid", "query_execution_with_result_transfer_seconds", "error")}}, indent=2))


if __name__ == "__main__":
    main()
