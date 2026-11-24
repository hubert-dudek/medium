# Databricks notebook source
from urllib.parse import quote

from databricks.sdk import WorkspaceClient

dbutils.widgets.text("provider", "", "ChatGPT provider (catalog.schema.name)")
provider = dbutils.widgets.get("provider").strip()
if not provider:
    raise ValueError("Enter the full name of your ChatGPT provider.")

# COMMAND ----------

workspace = WorkspaceClient()
path = "/api/2.1/unity-catalog/model-provider-services/" + quote(provider, safe="")
service = workspace.api_client.do("GET", path)
config = service["config"]
if config["provider_type"] != "EXTERNAL_MODEL_PROVIDER_TYPE_OPENAI" or "relayed" not in config.get("openai", {}):
    raise ValueError(f"{provider} does not use a ChatGPT subscription.")
print(provider, config["provider_type"])

# COMMAND ----------

print(f'ug codex --provider "{provider}" --workspace "{workspace.config.host}"')
