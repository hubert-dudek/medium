# Databricks notebook source
# Widgets expected by the deployment job:
dbutils.widgets.text("model_name", "")
dbutils.widgets.text("model_version", "")
model_name    = dbutils.widgets.get("model_name")
model_version = dbutils.widgets.get("model_version")

# The approval task name must start with "approval"; here we use "Approval_Check".
APPROVAL_TAG_KEY   = "Approval_Check"
REQUIRED_TAG_VALUE = "Approved"

from mlflow.tracking import MlflowClient
client = MlflowClient()
mv = client.get_model_version(name=model_name, version=model_version)

# Tag reading: ModelVersion object includes tags; if not present yet, fail the task.
tags = dict(mv.tags) if hasattr(mv, "tags") else {}
if tags.get(APPROVAL_TAG_KEY) != REQUIRED_TAG_VALUE:
    raise RuntimeError(
        f"Awaiting approval. On the UC Model Version page, click 'Approve' "
        f"for task '{APPROVAL_TAG_KEY}' to set UC tag {APPROVAL_TAG_KEY}={REQUIRED_TAG_VALUE}."
    )

print("✅ Approved — proceeding to deployment.")
