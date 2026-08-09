# Databricks notebook source
from __future__ import annotations

from datetime import datetime

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
schema = widget("schema", "transaction_processing_dev")

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"{quote_identifier(catalog)}.{quote_identifier(schema)}"
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name('transactions')} (
      event_id STRING,
      transaction_id BIGINT,
      customer_id BIGINT,
      event_time TIMESTAMP,
      source_received_at TIMESTAMP,
      status STRING,
      amount DECIMAL(18, 2),
      source_batch INT,
      loaded_at TIMESTAMP
    ) USING DELTA
    """
)

watermark = spark.sql(
    f"""
    SELECT MAX(event_time) AS watermark
    FROM {table_name('transactions')}
    """
).first()["watermark"] or datetime(1900, 1, 1)

rows_to_load = (
    spark.table(f"{catalog}.{schema}.transaction_events")
    .filter(F.col("event_time") > F.lit(watermark))
    .select(
        "event_id",
        "transaction_id",
        "customer_id",
        "event_time",
        F.col("received_at").alias("source_received_at"),
        "status",
        "amount",
        "source_batch",
        F.current_timestamp().alias("loaded_at"),
    )
)

rows_to_load.write.mode("append").saveAsTable(
    f"{catalog}.{schema}.transactions"
)

spark.sql(
    f"""
    SELECT
      source_batch,
      COUNT(*) AS row_count,
      MIN(event_time) AS first_event_time,
      MAX(event_time) AS last_event_time,
      MAX(loaded_at) AS loaded_at
    FROM {table_name('transactions')}
    GROUP BY source_batch
    ORDER BY source_batch DESC
    LIMIT 10
    """
).show(truncate=False)

