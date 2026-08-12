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
    CREATE TABLE IF NOT EXISTS {table_name('device_readings')} (
      id TINYINT,
      device_id STRING,
      reading_time TIMESTAMP,
      reading_value DECIMAL(10, 2),
      source_created_at TIMESTAMP,
      loaded_at TIMESTAMP
    ) USING DELTA
    """
)

spark.sql(
    f"""
    MERGE INTO {table_name('device_readings')} AS target
    USING (
      SELECT
        id,
        device_id,
        reading_time,
        reading_value,
        created_at AS source_created_at,
        current_timestamp() AS loaded_at
      FROM {table_name('device_readings_source')}
    ) AS source
    ON target.id = source.id
    WHEN NOT MATCHED THEN INSERT (
      id,
      device_id,
      reading_time,
      reading_value,
      source_created_at,
      loaded_at
    ) VALUES (
      source.id,
      source.device_id,
      source.reading_time,
      source.reading_value,
      source.source_created_at,
      source.loaded_at
    )
    """
)
