# Databricks notebook source
# MAGIC %pip install "databricks-agentbricks>=0.6.0"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks_agentkit import AgentKitClient

client = AgentKitClient(WorkspaceClient())
store = client.memory_stores.create("news-memory-20261021")

# COMMAND ----------

store.put(
    actor_id="news-demo-user",
    path="/preferences/news.md",
    content="Prefers two short sentences and one screenshot per news item.",
    description="Preferred news format",
)

# COMMAND ----------

store = AgentKitClient(WorkspaceClient()).memory_stores.get("news-memory-20261021")
results = store.search(actor_id="news-demo-user", query="Preferred news format", limit=1)

for result in results:
    print(result.memory.content)
