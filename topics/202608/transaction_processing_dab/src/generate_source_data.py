# Databricks notebook source
from __future__ import annotations

from datetime import timedelta

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
rows_per_batch = int(widget("rows_per_batch", "500"))

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"{quote_identifier(catalog)}.{quote_identifier(schema)}"
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name('transaction_events')} (
      event_id STRING,
      transaction_id BIGINT,
      customer_id BIGINT,
      event_time TIMESTAMP,
      received_at TIMESTAMP,
      status STRING,
      amount DECIMAL(18, 2),
      source_batch INT
    ) USING DELTA
    """
)

last_batch = spark.sql(
    f"""
    SELECT
      COALESCE(MAX(source_batch), 0) AS source_batch,
      MAX(event_time) AS event_time
    FROM {table_name('transaction_events')}
    """
).first()

batch_id = int(last_batch["source_batch"]) + 1

if last_batch["event_time"] is None:
    batch_time = spark.sql(
        """
        SELECT date_trunc('HOUR', current_timestamp()) - INTERVAL 8 HOURS AS batch_time
        """
    ).first()["batch_time"]
else:
    batch_time = last_batch["event_time"] + timedelta(hours=1)

transaction_id = (
    F.lit(batch_id * 1_000_000).cast("long")
    + F.col("id")
    + F.lit(1)
)

rows = spark.range(rows_per_batch)

if batch_id > 6:
    rows = rows.withColumn(
        "event_time",
        F.when(
            F.pmod(F.col("id"), F.lit(5)) == F.lit(0),
            F.lit(batch_time),
        ).otherwise(F.lit(batch_time - timedelta(hours=4))),
    )
else:
    rows = rows.withColumn("event_time", F.lit(batch_time))

events = rows.select(
    F.concat(F.lit("E"), F.lit(batch_id), F.lit("-"), F.col("id")).alias(
        "event_id"
    ),
    transaction_id.alias("transaction_id"),
    (
        F.lit(1000)
        + F.pmod(F.xxhash64(transaction_id), F.lit(9000))
    ).cast("long").alias("customer_id"),
    F.col("event_time").cast("timestamp").alias("event_time"),
    F.current_timestamp().alias("received_at"),
    F.when(
        F.pmod(transaction_id, F.lit(20)) == F.lit(0),
        F.lit("CANCELLED"),
    )
    .otherwise(F.lit("COMPLETED"))
    .alias("status"),
    (
        (
            F.lit(1000)
            + F.pmod(
                F.xxhash64(transaction_id, F.lit("amount")),
                F.lit(99000),
            )
        )
        / F.lit(100)
    ).cast("decimal(18,2)").alias("amount"),
    F.lit(batch_id).cast("int").alias("source_batch"),
)

events.write.mode("append").saveAsTable(
    f"{catalog}.{schema}.transaction_events"
)
