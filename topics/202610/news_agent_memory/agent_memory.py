# Databricks notebook source
# MAGIC %pip install "databricks-agentbricks>=0.6.0"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks_agentkit import AgentKitClient

client = AgentKitClient(WorkspaceClient())
stores = {store.id: store for store in client.memory_stores.list()}
store = stores.get("news-memory-20261025")
if store is None:
    store = client.memory_stores.create("news-memory-20261025")

# COMMAND ----------

store.put(
    actor_id="user-123",
    path="/preferences/food.md",
    content="User prefers Mexican recipes",
    description="Food preferences",
)

saved = store.list(actor_id="user-123", path="/preferences/food.md")
display(spark.createDataFrame(
    [(m.content,) for m in saved], ["saved_memory"]
))

# COMMAND ----------

fresh_client = AgentKitClient(WorkspaceClient())
store = fresh_client.memory_stores.get(
    "news-memory-20261025"
)
results = store.search(
    actor_id="user-123",
    query="What should I cook for dinner?",
    limit=1,
)

display(spark.createDataFrame(
    [(r.memory.content,) for r in results], ["recalled_memory"]
))
