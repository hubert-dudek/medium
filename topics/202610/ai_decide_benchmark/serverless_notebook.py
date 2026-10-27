# Databricks notebook source
import json
import os
import platform
from pathlib import Path
import re
import sys
import time
from datetime import datetime, timezone

dbutils.widgets.text("source_dir", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("job_id", "")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("task_run_id", "")

source_dir = Path(dbutils.widgets.get("source_dir") or os.getcwd()).resolve()
if not all((source_dir / name).is_file() for name in ("corpus.py", "benchmark.py")):
    raise ValueError("source_dir must contain corpus.py and benchmark.py")
sys.path.insert(0, str(source_dir))

from corpus import CASES, validate
from benchmark import question

# COMMAND ----------

run_id = dbutils.widgets.get("run_id")
if not re.fullmatch(r"[A-Za-z0-9_]{1,128}", run_id):
    raise ValueError("A run_id containing only letters, numbers, or underscores is required")

corpus = validate()
expected_sha256 = "f01dcc9e9f2d3f4de321e879e32c72d66794697677dbd3de21d087f2f964693e"
if corpus["sha256"] != expected_sha256:
    raise ValueError("Corpus differs from the frozen five-post benchmark")
assert len(CASES) == 5
assert len({case["id"] for case in CASES}) == 5

questions = question()
parameters = {"questions": json.dumps(questions)}
for index, case in enumerate(CASES):
    parameters[f"id_{index}"] = case["id"]
    parameters[f"text_{index}"] = case["text"]
    parameters[f"expected_{index}"] = "approved" if case["expected_approved"] else "refused"

# COMMAND ----------

query = (
    f"/* run_id={run_id}; phase=serverless_benchmark */\n"
    + "WITH posts AS (SELECT * FROM VALUES\n"
    + ",\n".join(f"(:id_{index}, :text_{index}, :expected_{index})" for index in range(len(CASES)))
    + "\nAS p(case_id, post_text, expected_label))\n"
    + "SELECT case_id, expected_label, "
    + "to_json(ai_decide(post_text, :questions, map('version', '1.0'))) AS decision_json "
    + "FROM posts"
)

environment = {
    "python_version": platform.python_version(),
    "spark_version": spark.version,
    "spark_session_class": type(spark).__module__ + "." + type(spark).__name__,
    "databricks_runtime_version": os.environ.get("DATABRICKS_RUNTIME_VERSION"),
    "job_id": dbutils.widgets.get("job_id") or None,
    "job_run_id": dbutils.widgets.get("job_run_id") or None,
    "task_run_id": dbutils.widgets.get("task_run_id") or None,
}

# COMMAND ----------

if globals().get("_ai_decide_submission_started"):
    raise RuntimeError("Inference was already submitted in this Python session")

report = {
    "run_id": run_id,
    "model": "ai_decide_serverless_job",
    "corpus": corpus,
    "environment": environment,
    "questions": questions,
    "options": {"version": "1.0"},
    "expected_count": len(CASES),
    "timing_metric": "query_execution_with_result_transfer_seconds",
    "startup_included": False,
    "status": "submitted",
    "started_at": datetime.now(timezone.utc).isoformat(),
}
_ai_decide_submission_started = True
started = time.perf_counter()
try:
    collected = spark.sql(query, args=parameters).collect()
except Exception as error:
    report["status"] = "error"
    report["error"] = {"type": type(error).__name__, "message": str(error)}
    collected = None
finally:
    report["query_execution_with_result_transfer_seconds"] = time.perf_counter() - started
    report["finished_at"] = datetime.now(timezone.utc).isoformat()

# COMMAND ----------

if collected is not None:
    rows = []
    for row in collected:
        raw_json = row["decision_json"]
        try:
            raw = json.loads(raw_json) if raw_json is not None else None
            label = raw.get("response", {}).get("answers", {}).get("moderation", {}).get("choice") if isinstance(raw, dict) else None
            error = raw.get("error_message") if isinstance(raw, dict) else None
        except (TypeError, ValueError, AttributeError) as parse_error:
            raw, label, error = None, None, str(parse_error)
        valid = isinstance(label, str) and label in {"approved", "refused"} and not error
        rows.append({
            "case_id": row["case_id"],
            "expected": row["expected_label"],
            "label": label,
            "valid": valid,
            "correct": valid and label == row["expected_label"],
            "error": error,
            "raw": raw,
            "raw_json": raw_json,
        })
    report["rows"] = rows
    report["count"] = len(rows)
    report["correct"] = sum(row["correct"] for row in rows)
    report["invalid"] = sum(not row["valid"] for row in rows)
    report["status"] = "succeeded" if len(rows) == 5 and {row["case_id"] for row in rows} == {case["id"] for case in CASES} else "incomplete_result"

# COMMAND ----------

dbutils.notebook.exit(json.dumps(report, ensure_ascii=False))
