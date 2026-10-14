# Databricks notebook source
import json
import re
from uuid import uuid4

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "state_repartitioning_demo")
dbutils.widgets.text("volume", "checkpoints")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
assert all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) for value in [catalog, schema, volume])

demo_id = uuid4().hex[:12]
source_table = f"{catalog}.{schema}.stream_events_{demo_id}"
target_table = f"{catalog}.{schema}.stream_counts_{demo_id}"
checkpoint = f"/Volumes/{catalog}/{schema}/{volume}/notebook/{demo_id}"
history = []
progress_history = []

# COMMAND ----------

spark.conf.set("spark.sql.streaming.stateStore.partitions", "2")
display(spark.sql("SELECT current_version() AS engine"))

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
spark.range(0).selectExpr("CAST(id % 4 AS STRING) AS key").write.format("delta").saveAsTable(source_table)

# COMMAND ----------

def append_events():
    for start in [0, 10]:
        (
            spark.range(start, start + 10)
            .selectExpr("CAST(id % 4 AS STRING) AS key")
            .coalesce(1)
            .write.format("delta")
            .mode("append")
            .saveAsTable(source_table)
        )


def start_query():
    return (
        spark.readStream.option("maxFilesPerTrigger", 1)
        .table(source_table)
        .groupBy("key")
        .count()
        .writeStream.format("delta")
        .outputMode("complete")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(target_table)
    )

# COMMAND ----------

def verify_phase(query, partitions, expected_count):
    try:
        assert query.awaitTermination(300), "Streaming phase timed out"
    finally:
        if query.isActive:
            query.stop()

    progress = [
        json.loads(item.json) if hasattr(item, "json") else item
        for item in query.recentProgress
    ]
    progress_history.append(progress)
    operators = [p["stateOperators"][0] for p in progress if p.get("stateOperators")]
    assert operators, progress
    last_state = operators[-1]
    assert last_state["numShufflePartitions"] == partitions, last_state
    assert any(key.startswith("rocksdb") for key in last_state["customMetrics"]), last_state
    assert spark.table(source_table).count() == expected_count * 4

    counts = {row["key"]: row["count"] for row in spark.table(target_table).collect()}
    assert counts == {str(key): expected_count for key in range(4)}, counts
    repartitions = [
        p["durationMs"]["controlBatch.REPARTITION"]
        for p in progress
        if "controlBatch.REPARTITION" in p.get("durationMs", {})
    ]
    if history:
        assert str(query.id) == history[0]["query_id"]
        assert str(query.runId) not in [row["run_id"] for row in history]
        assert repartitions, progress

    history.append({
        "partitions": last_state["numShufflePartitions"],
        "state_keys": last_state["numRowsTotal"],
        "total_events": sum(counts.values()),
        "repartition_ms": sum(repartitions),
        "query_id": str(query.id),
        "run_id": str(query.runId),
    })
    display(spark.table(target_table).orderBy("key"))
    print(json.dumps(history[-1], indent=2))

# COMMAND ----------

append_events()
query = start_query()
verify_phase(query, partitions=2, expected_count=5)

# COMMAND ----------

query.stop()
spark.conf.set("spark.sql.streaming.stateStore.partitions", "4")
append_events()
query = start_query()
verify_phase(query, partitions=4, expected_count=10)

# COMMAND ----------

query.stop()
spark.conf.set("spark.sql.streaming.stateStore.partitions", "2")
append_events()
query = start_query()
verify_phase(query, partitions=2, expected_count=15)

# COMMAND ----------

assert not query.isActive
display(spark.createDataFrame(history).select("partitions", "state_keys", "total_events", "repartition_ms"))
print(json.dumps({"checkpoint": checkpoint, "phases": history, "progress": progress_history}, indent=2))
