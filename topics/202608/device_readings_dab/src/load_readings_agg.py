# Databricks notebook source
from __future__ import annotations


def widget(name: str, default: str) -> str:
    try:
        return dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name)


def quote_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def table_name(name: str) -> str:
    return (
        f"{quote_identifier(catalog)}."
        f"{quote_identifier(schema)}."
        f"{quote_identifier(name)}"
    )


catalog = widget("catalog", "main")
schema = widget("schema", "device_readings_dev")

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name('device_readings_agg')} (
      device_id STRING,
      count BIGINT
    )
    """
)

spark.sql(
    f"""
    MERGE INTO {table_name('device_readings_agg')} AS target
    USING (
      SELECT
        device_id,
        count(*) AS count
      FROM {table_name('device_readings')}
      GROUP BY device_id
    ) AS source
    ON target.device_id = source.device_id
    WHEN NOT MATCHED THEN INSERT (
      device_id,
      count
    ) VALUES (
      source.device_id,
      1
    )
    WHEN MATCHED THEN UPDATE SET target.count = source.count
    """
)
