"""Apply the Beta API fields to this bundle's existing job after each deployment.

Run with `databricks bundle run apply_environment`, which supplies authentication
and the resolved bundle values. No extra package or personal token in the code.
"""
import copy
import json
import os
import shutil
import subprocess
import sys


def required(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing {name}; run this script through the bundle")
    return value


def api(method, path, payload=None):
    cli = shutil.which("databricks")
    if not cli:
        raise RuntimeError("Put the Databricks CLI on PATH before running the bundle")
    command = [cli, "api", method, path, "--output", "json"]
    if payload is not None:
        # The payload contains only this demo's non-secret application settings.
        command += ["--json", json.dumps(payload)]
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode:
        # CLI debug logging is deliberately disabled; never print the environment.
        raise RuntimeError(result.stderr.strip() or "Databricks API request failed")
    return json.loads(result.stdout) if result.stdout.strip() else {}


def main():
    cli = shutil.which("databricks")
    if not cli:
        raise RuntimeError("Put the Databricks CLI on PATH before running the bundle")
    summary = subprocess.run(
        [cli, "bundle", "summary", "-t", required("ENV_DEMO_TARGET"), "--output", "json"],
        text=True, capture_output=True, check=True,
    )
    job_id = int(json.loads(summary.stdout)["resources"]["jobs"]["env_demo"]["id"])
    files_root = required("ENV_DEMO_FILES").rstrip("/")
    app_env = required("ENV_DEMO_APP_ENV")
    log_level = required("ENV_DEMO_LOG_LEVEL").upper()
    if app_env == "from-file":
        raise ValueError("Choose an app_env other than from-file for the precedence experiment")
    if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ValueError("Use a standard Python log level")

    current = api("get", f"/api/2.2/jobs/get?job_id={job_id}")
    tasks = copy.deepcopy(current["settings"]["tasks"])
    expected_keys = {"read_environment", "without_entry", "udf_boundary"}
    if {task["task_key"] for task in tasks} != expected_keys:
        raise RuntimeError("The job does not match this demo; refusing to change it")
    for task in tasks:
        if not task["notebook_task"]["notebook_path"].startswith(files_root + "/"):
            raise RuntimeError("A task is outside this bundle's files; refusing to change it")
        if task["task_key"] != "without_entry":
            task["environment_variables_key"] = "app_config"
        else:
            task.pop("environment_variables_key", None)

    entries = [{
        "environment_variables_key": "app_config",
        "spec": {
            "variables": {
                "APP_ENV": app_env,
                "LOG_LEVEL": log_level,
                "ARTICLE_ENV_MARKER": "serverless-env-demo",
            },
            "files": [files_root + "/config/application.env"],
        },
    }]
    # jobs/update replaces matching task entries, so retain the full task objects.
    api("post", "/api/2.2/jobs/update", {
        "job_id": job_id,
        "new_settings": {"environment_variables": entries, "tasks": tasks},
    })
    saved = api("get", f"/api/2.2/jobs/get?job_id={job_id}")["settings"]
    if saved.get("environment_variables") != entries:
        raise RuntimeError("Environment entries were not retained by the Jobs API")
    for task in saved["tasks"]:
        expected = None if task["task_key"] == "without_entry" else "app_config"
        if task.get("environment_variables_key") != expected:
            raise RuntimeError("A task selection was not retained by the Jobs API")
    print(json.dumps({"job_id": job_id, "app_env": app_env, "log_level": log_level,
                      "api_readback": "verified"}, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)
