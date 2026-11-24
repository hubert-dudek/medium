# Databricks notebook source
import pandas as pd
from databricks.sdk import WorkspaceClient

history = WorkspaceClient().bundle_deployments
bundle_name = "news-deployment-history-20261021"
deployments = [
    item for item in history.list_deployments()
    if item.display_name == bundle_name and item.target_name == "news"
]
if len(deployments) != 1:
    raise ValueError(f"Expected one deployment of {bundle_name} to the news target, found {len(deployments)}")
deployment = deployments[0]

# COMMAND ----------

versions = pd.DataFrame(
    [item.as_dict() for item in history.list_versions(deployment.name)],
    columns=["version_id", "version_type", "status", "create_time", "created_by"],
)
display(versions)

# COMMAND ----------

resources = pd.DataFrame(
    [item.as_dict() for item in history.list_resources(deployment.name)],
    columns=["resource_type", "resource_key", "last_version_id", "last_action_type"],
)
display(resources)
