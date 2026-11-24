# Databricks notebook source
# MAGIC %pip install databricks-agentbricks==0.7.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from hashlib import sha256

from databricks.sdk import WorkspaceClient
from databricks_agentkit import AgentKitClient

workspace = WorkspaceClient()
client = AgentKitClient(workspace)
user_hash = sha256(workspace.current_user.me().id.encode()).hexdigest()[:10]
store_id = f"news-202621-recipes-{user_hash}"
store = next((item for item in client.memory_stores.list() if item.id == store_id), None)
description = "Temporary news 202621 video demo"
if store is None:
    store = client.memory_stores.create(store_id, description=description)
elif store.description != description:
    raise ValueError(f"{store_id} belongs to another demonstration.")
print(store.id, store.storage_backend)

# COMMAND ----------

actor = "recording-user"
path = "/preferences/food.md"
store.put(actor_id=actor, path=path, content="User prefers Mexican recipes", description="Food preferences")
saved = store.list(actor_id=actor, path=path)
display(spark.createDataFrame([(row.content,) for row in saved], "saved_memory STRING"))

# COMMAND ----------

fresh_client = AgentKitClient(WorkspaceClient())
saved_store = fresh_client.memory_stores.get(store_id)
matches = saved_store.search(actor_id=actor, query="What should I cook for dinner?", limit=1)
display(spark.createDataFrame([(row.memory.content,) for row in matches], "recalled_memory STRING"))

# COMMAND ----------

dbutils.widgets.dropdown("remove_after_filming", "no", ["no", "yes"])
if dbutils.widgets.get("remove_after_filming") == "yes":
    store.delete()
    print(f"Removed {store_id}")
