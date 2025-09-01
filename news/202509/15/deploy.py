# Databricks notebook source
# Widgets expected by the deployment job:
dbutils.widgets.text("model_name", "")
dbutils.widgets.text("model_version", "")
dbutils.widgets.text("endpoint_name", "")  # optional; defaults from model name

model_name    = dbutils.widgets.get("model_name")
model_version = dbutils.widgets.get("model_version")
endpoint_name = dbutils.widgets.get("endpoint_name") or (model_name.replace(".", "-") + "-endpoint")

CATALOG = "hub"
SCHEMA  = "default"
TABLE   = f"{CATALOG}.{SCHEMA}.re_prices"      # UC feature table with PRIMARY KEY (id)
UC_MODEL= f"{CATALOG}.{SCHEMA}.re_price_lr"    # UC (3-part) model name

# Optional: install SDK if your cluster image doesn’t have it
# %pip install -U databricks-sdk
# dbutils.library.restartPython()

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

w = WorkspaceClient()

served = ServedEntityInput(
    name="regr",
    entity_name=model_name,        # UC model  (<catalog>.<schema>.<model>)
    entity_version=model_version,  # version number from deployment job
    scale_to_zero_enabled=True,
)

try:
    # Create (first time)
    w.serving_endpoints.create_and_wait(
        name=endpoint_name,
        config=EndpointCoreConfigInput(served_entities=[served]),
    )
    print(f"✅ Created serving endpoint: {endpoint_name}")
except Exception as e:
    # Update (if endpoint already exists)
    w.serving_endpoints.update_config_and_wait(
        name=endpoint_name,
        served_entities=[served],
    )
    print(f"✅ Updated serving endpoint '{endpoint_name}' to {model_name} v{model_version}")

