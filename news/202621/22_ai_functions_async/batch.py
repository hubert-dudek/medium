# Databricks notebook source
from pathlib import Path
from time import monotonic, sleep
import json

from databricks.sdk import WorkspaceClient

dbutils.widgets.text("catalog", "sbx_test_data")
dbutils.widgets.text("schema", "news_202621")
dbutils.widgets.text("volume", "inputs")
catalog, schema, volume = (dbutils.widgets.get(key) for key in ("catalog", "schema", "volume"))

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:name)", args={"name": f"{catalog}.{schema}"})
spark.sql("CREATE VOLUME IF NOT EXISTS IDENTIFIER(:name)", args={"name": f"{catalog}.{schema}.{volume}"})
folder = Path("/Volumes") / catalog / schema / volume / "support_ticket_batch"
output = folder / "output"
output.mkdir(parents=True, exist_ok=True)
requests = folder / "requests.jsonl"
requests.write_bytes(Path("requests.jsonl").read_bytes())
print(requests.read_text())

# COMMAND ----------

api = WorkspaceClient().api_client
endpoint = "/api/2.0/ai-functions/batches"
batch = api.do("POST", endpoint, body={
    "endpoint": "ai_classify",
    "input_ref": str(requests),
    "params": {"labels": ["billing", "technical"], "options": {"version": "2.1"}},
    "output_ref": str(output) + "/",
})
print(batch["name"], batch["status"])

# COMMAND ----------

deadline = monotonic() + 300
while True:
    status = api.do("GET", f"{endpoint}/{batch['name']}")
    print(status["status"], status.get("request_counts", {}))
    if status["status"] == "COMPLETED":
        break
    if status["status"] in {"FAILED", "CANCELLED"}:
        raise RuntimeError(status)
    if monotonic() >= deadline:
        raise TimeoutError(f"Run this polling cell again to resume {batch['name']}.")
    sleep(5)

# COMMAND ----------

rows = [json.loads(line) for line in Path(status["output_ref"]).read_text().splitlines()]
for row in rows:
    print(row["custom_id"], row.get("error") or row["response"])
