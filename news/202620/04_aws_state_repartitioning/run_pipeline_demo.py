import argparse
import json
import re
import subprocess
import tempfile
import time
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--warehouse-id", required=True)
parser.add_argument("--profile", default="DEFAULT")
parser.add_argument("--cli", default="databricks")
args = parser.parse_args()
bundle_dir = Path(__file__).resolve().parent


def cli(*command, body=None, parse=True):
    with tempfile.TemporaryDirectory() as directory:
        command = [args.cli, *command, "--profile", args.profile]
        if body is not None:
            request = Path(directory) / "request.json"
            request.write_text(json.dumps(body), encoding="utf-8")
            command += ["--json", "@" + str(request)]
        if parse:
            command += ["--output", "json"]
        result = subprocess.run(command, cwd=bundle_dir, check=True, capture_output=True, text=True, timeout=180)
        return json.loads(result.stdout) if parse and result.stdout.strip() else result.stdout


def sql(statement):
    result = cli("api", "post", "/api/2.0/sql/statements", body={
        "warehouse_id": args.warehouse_id,
        "statement": statement,
        "wait_timeout": "10s",
        "on_wait_timeout": "CONTINUE",
    })
    deadline = time.monotonic() + 180
    while result["status"]["state"] in ("PENDING", "RUNNING"):
        if time.monotonic() > deadline:
            cli("api", "post", f"/api/2.0/sql/statements/{result['statement_id']}/cancel")
            raise TimeoutError("SQL statement timed out")
        time.sleep(3)
        result = cli("api", "get", f"/api/2.0/sql/statements/{result['statement_id']}")
    assert result["status"]["state"] == "SUCCEEDED", result["status"]
    assert not result.get("manifest", {}).get("truncated", False)
    return result.get("result", {}).get("data_array", [])


def update_pipeline():
    result = cli("api", "post", f"/api/2.0/pipelines/{pipeline_id}/updates", body={"full_refresh": False})
    update_id = result["update_id"]
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        update = cli("api", "get", f"/api/2.0/pipelines/{pipeline_id}/updates/{update_id}")["update"]
        if update["state"] == "COMPLETED":
            return update_id
        assert update["state"] not in ("FAILED", "CANCELED"), update
        time.sleep(5)
    raise TimeoutError("Pipeline update timed out")


def read_progress(update_id):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        rows = sql(f"""
            SELECT details:stream_progress.progress_json
            FROM event_log('{pipeline_id}')
            WHERE event_type = 'stream_progress' AND origin.update_id = '{update_id}'
            ORDER BY timestamp
        """)
        progress = [json.loads(row[0]) for row in rows]
        if progress and progress[-1].get("stateOperators"):
            return progress
        time.sleep(3)
    raise TimeoutError("Streaming progress was not available")


def stop_pipeline():
    cli("api", "post", f"/api/2.0/pipelines/{pipeline_id}/stop")
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        if cli("pipelines", "get", pipeline_id)["state"] == "IDLE":
            return
        time.sleep(3)
    raise TimeoutError("Pipeline did not become idle")


summary = cli("bundle", "summary")
pipeline = summary["resources"]["pipelines"]["state_repartitioning"]
pipeline_id = pipeline["id"]
catalog, schema = pipeline["catalog"], pipeline["schema"]
assert all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) for value in (catalog, schema))
source = f"{catalog}.{schema}.pipeline_events"
target = f"{catalog}.{schema}.pipeline_unique_events"
warehouse = cli("warehouses", "get", args.warehouse_id)
stop_warehouse = warehouse["state"] == "STOPPED"
history = []

try:
    sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    sql(f"CREATE TABLE IF NOT EXISTS {source} (event_id BIGINT, batch_id INT) USING DELTA")
    baseline, next_event, next_batch = map(int, sql(f"""
        SELECT COUNT(DISTINCT event_id), COALESCE(MAX(event_id), -1) + 1,
               COALESCE(MAX(batch_id), 0) + 1 FROM {source}
    """)[0])
    target_exists = int(sql(f"""
        SELECT COUNT(*) FROM {catalog}.information_schema.tables
        WHERE table_schema = '{schema}' AND table_name = 'pipeline_unique_events'
    """)[0][0])
    if target_exists:
        counts = list(map(int, sql(f"SELECT COUNT(*), COUNT(DISTINCT event_id) FROM {target}")[0]))
        assert counts == [baseline, baseline], "Process existing source rows before this demo"
        assert not sql(f"SELECT event_id FROM {source} EXCEPT SELECT event_id FROM {target}")
    else:
        assert baseline == 0, "Process existing source rows before this demo"

    for phase, partitions in enumerate((2, 4, 2)):
        start = next_event + phase * 4
        sql(f"""
            INSERT INTO {source}
            SELECT id, {next_batch + phase} FROM range({start}, {start + 8})
        """)
        cli("bundle", "deploy", "--auto-approve", "--var", f"state_partitions={partitions}", parse=False)
        update_id = update_pipeline()
        progress = read_progress(update_id)
        state = progress[-1]["stateOperators"][0]
        expected = baseline + 8 + phase * 4
        assert state["numShufflePartitions"] == partitions, state
        assert state["numRowsTotal"] == expected, state
        assert any(key.startswith("rocksdb") for key in state["customMetrics"]), state
        counts = list(map(int, sql(f"SELECT COUNT(*), COUNT(DISTINCT event_id) FROM {target}")[0]))
        assert counts == [expected, expected], counts
        missing = sql(f"SELECT event_id FROM {source} EXCEPT SELECT event_id FROM {target}")
        assert not missing, missing
        durations = [p["durationMs"]["controlBatch.REPARTITION"] for p in progress
                     if "controlBatch.REPARTITION" in p["durationMs"]]
        duplicates = sum(p["stateOperators"][0]["customMetrics"]["numDroppedDuplicateRows"] for p in progress)
        assert duplicates == (0 if phase == 0 else 4), duplicates
        if history:
            assert durations, progress
            assert progress[-1]["id"] == history[0]["query_id"]
            assert progress[-1]["runId"] not in [row["run_id"] for row in history]
        history.append({
            "phase": phase + 1,
            "partitions": state["numShufflePartitions"],
            "state_rows": state["numRowsTotal"],
            "duplicates_dropped": duplicates,
            "repartition_ms": sum(durations) if durations else None,
            "output_rows": counts[0],
            "query_id": progress[-1]["id"],
            "run_id": progress[-1]["runId"],
        })
        print(json.dumps(history[-1], indent=2), flush=True)
finally:
    try:
        stop_pipeline()
    finally:
        if stop_warehouse:
            cli("api", "post", f"/api/2.0/sql/warehouses/{args.warehouse_id}/stop")
