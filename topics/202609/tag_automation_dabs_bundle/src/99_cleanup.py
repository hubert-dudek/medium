# Databricks notebook source
# MAGIC %md
# MAGIC # Cleanup the Tag Automations experiment
# MAGIC
# MAGIC The schema is always removed. The account-level governed-tag definitions are removed only when `drop_governed_tags=true`.

# COMMAND ----------

from __future__ import annotations

import re

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Existing catalog")
dbutils.widgets.text("schema_name", "tags_experiment", "Experiment schema")
dbutils.widgets.dropdown(
    "drop_governed_tags",
    "false",
    ["true", "false"],
    "Drop ta_demo_* governed tags",
)

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
drop_governed_tags = dbutils.widgets.get("drop_governed_tags").lower() == "true"

if not catalog_name:
    raise ValueError("catalog_name cannot be empty")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,254}", schema_name):
    raise ValueError("Invalid schema_name")


def qi(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def qname(*parts: str) -> str:
    return ".".join(qi(part) for part in parts)


spark.sql(f"DROP SCHEMA IF EXISTS {qname(catalog_name, schema_name)} CASCADE")
print(f"Dropped schema: {catalog_name}.{schema_name}")

if drop_governed_tags:
    for tag_key in ["ta_demo_pii", "ta_demo_documentation"]:
        try:
            spark.sql(f"DROP GOVERNED TAG {qi(tag_key)}")
            print(f"Dropped governed tag: {tag_key}")
        except Exception as exc:
            text = str(exc).lower()
            if "not_found" in text or "does not exist" in text or "not found" in text:
                print(f"Governed tag was already absent: {tag_key}")
            else:
                raise
else:
    print("Governed-tag definitions were retained. Set drop_governed_tags=true to remove them.")

