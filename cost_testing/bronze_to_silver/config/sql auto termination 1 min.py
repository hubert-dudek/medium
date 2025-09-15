# Databricks notebook source
"""
create_sql_warehouses.py
------------------------
Create three SQL warehouses (XXS, XS, S) with 1‑minute auto‑stop.

Prerequisites
-------------
1. `pip install databricks-sdk`
2. Configure authentication (DATABRICKS_TOKEN / profile, or run inside a Databricks notebook).

Usage
-----
python create_sql_warehouses.py
"""
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql

# ----------------------------------------------------------------------
# Initialise a WorkspaceClient with default authentication settings.
# ----------------------------------------------------------------------
w = WorkspaceClient()          # or WorkspaceClient(profile="my-profile")

# ----------------------------------------------------------------------
# Warehouse specifications you want to create
# ----------------------------------------------------------------------
WAREHOUSES = [
    ("SQL_XXS", "2X-Small"),   # maps to “2X‑Small” cluster size :contentReference[oaicite:1]{index=1}
    ("SQL_XS",  "X-Small"),    # maps to “X‑Small”
    ("SQL_S",   "Small")       # maps to “Small”
]

# ----------------------------------------------------------------------
# Helper: return an existing warehouse with the same name (if any)
# ----------------------------------------------------------------------
def find_existing(name: str):
    return next((wh for wh in w.warehouses.list() if wh.name == name), None)

# ----------------------------------------------------------------------
# Create the warehouses
# ----------------------------------------------------------------------
for wh_name, cluster_size in WAREHOUSES:
    if find_existing(wh_name):
        print(f"⚠️  Warehouse '{wh_name}' already exists – skipping.")
        continue

    # create_and_wait() blocks until the warehouse reaches RUNNING state
    wh = w.warehouses.create_and_wait(
        name=wh_name,
        cluster_size=cluster_size,
        auto_stop_mins=1,             # 1‑minute auto‑termination (serverless only) :contentReference[oaicite:2]{index=2}
        enable_serverless_compute=True,  # must be True for 1‑minute auto‑stop
        max_num_clusters=1              # single cluster per warehouse (tweak as needed)
    )

    print(f"✅ Created warehouse '{wh_name}' (id: {wh.id}, size: {cluster_size}).")

print("Finished.")

