# Databricks notebook source
import json
from pathlib import Path

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "job_runs_article")
dbutils.widgets.text("config_path", "")

namespace = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"
target = f"{namespace}.application_config"

# Read this small workspace file on the notebook driver.
config = json.loads(Path(dbutils.widgets.get("config_path")).read_text(encoding="utf-8"))
if not isinstance(config, list) or not config:
    raise ValueError("application.json must contain a nonempty list of configuration records")
if any(
    not isinstance(row, dict)
    or not isinstance(row.get("config_key"), str)
    or not row["config_key"].strip()
    or not isinstance(row.get("config_value"), str)
    or not isinstance(row.get("description", ""), str)
    for row in config
):
    raise ValueError("Each record needs a nonempty string config_key, string config_value, and optional string description")
if len({row["config_key"] for row in config}) != len(config):
    raise ValueError("config_key must be unique within application.json")

# COMMAND ----------
spark.sql(
    "CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:namespace)",
    args={"namespace": namespace},
).collect()
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS IDENTIFIER(:target)
    (
      config_key STRING,
      config_value STRING,
      description STRING,
      updated_at TIMESTAMP
    )
    USING DELTA
    """,
    args={"target": target},
).collect()

spark.createDataFrame(
    [(row["config_key"], row["config_value"], row.get("description", "")) for row in config],
    schema="config_key STRING, config_value STRING, description STRING",
).createOrReplaceTempView("incoming_application_config")

# Changed records get a new timestamp; unchanged records retain their timestamp.
# Keys removed from the JSON are deliberately retained in the target table.
spark.sql(
    """
    MERGE INTO IDENTIFIER(:target) AS target
    USING incoming_application_config AS source
    ON target.config_key = source.config_key
    WHEN MATCHED AND (
      NOT (target.config_value <=> source.config_value)
      OR NOT (target.description <=> source.description)
    ) THEN UPDATE SET
      target.config_value = source.config_value,
      target.description = source.description,
      target.updated_at = current_timestamp()
    WHEN NOT MATCHED THEN INSERT
      (config_key, config_value, description, updated_at)
    VALUES
      (source.config_key, source.config_value, source.description, current_timestamp())
    """,
    args={"target": target},
).collect()

# COMMAND ----------
display(spark.sql("SELECT * FROM IDENTIFIER(:target) ORDER BY config_key", args={"target": target}))
print(f"Merged {len(config)} configuration records into {target}")
