# Databricks notebook source
from __future__ import annotations

from pyspark.sql import functions as F


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

spark.conf.set("spark.sql.ansi.enabled", "true")


spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name('device_readings_source')} (
      id BIGINT,
      device_id STRING,
      reading_time TIMESTAMP,
      reading_value DECIMAL(10, 2),
      created_at TIMESTAMP
    )
    """
)

last_id = spark.sql(
    f"""
    SELECT COALESCE(MAX(CAST(id AS INT)), 0) AS last_id
    FROM {table_name('device_readings_source')}
    """
).first()["last_id"]

device_ids = F.array(
    F.lit("DEVICE-01"),
    F.lit("DEVICE-02"),
    F.lit("DEVICE-03"),
    F.lit("DEVICE-04"),
    F.lit("DEVICE-05"),
    F.lit("DEVICE-06"),
)

readings = spark.range(6).select(
    (
        F.lit(int(last_id))
        + F.col("id")
        + F.lit(1)
    ).cast("bigint").alias("id"),
    F.element_at(
        device_ids,
        (F.col("id") + F.lit(1)).cast("int"),
    ).alias("device_id"),
    F.date_trunc("minute", F.current_timestamp()).alias("reading_time"),
    F.round(F.rand() * F.lit(255), 2)
    .cast("decimal(10,2)")
    .alias("reading_value"),
    F.current_timestamp().alias("created_at"),
)

readings.write.mode("append").saveAsTable(
    f"{catalog}.{schema}.device_readings_source"
)
