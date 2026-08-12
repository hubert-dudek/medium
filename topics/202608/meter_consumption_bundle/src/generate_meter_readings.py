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
schema = widget("schema", "meter_consumption_dev")
rows_per_batch = int(widget("rows_per_batch", "48"))

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"{quote_identifier(catalog)}.{quote_identifier(schema)}"
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name('meter_readings')} (
      reading_id STRING,
      meter_id STRING,
      region STRING,
      period_start DATE,
      period_end DATE,
      units_consumed INT,
      source_batch BIGINT,
      received_at TIMESTAMP
    ) USING DELTA
    """
)

last_batch = spark.sql(
    f"""
    SELECT COALESCE(MAX(source_batch), 0) AS source_batch
    FROM {table_name('meter_readings')}
    """
).first()["source_batch"]

batch_id = int(last_batch) + 1

regions = F.array(
    F.lit("NORTH"),
    F.lit("SOUTH"),
    F.lit("EAST"),
    F.lit("WEST"),
)

readings = (
    spark.range(rows_per_batch)
    .withColumn(
        "service_days",
        F.floor(F.rand() * F.lit(256)).cast("int"),
    )
    .withColumn("period_end", F.current_date())
    .withColumn(
        "period_start",
        F.expr("date_sub(period_end, service_days)"),
    )
    .select(
        F.concat(
            F.lit(f"B{batch_id}-"),
            F.format_string("%05d", F.col("id")),
        ).alias("reading_id"),
        F.format_string(
            "M%05d",
            F.pmod(
                F.xxhash64(F.col("id"), F.lit(batch_id)),
                F.lit(5000),
            ),
        ).alias("meter_id"),
        F.element_at(
            regions,
            (
                F.pmod(
                    F.xxhash64(F.col("id"), F.lit("region")),
                    F.lit(4),
                )
                + F.lit(1)
            ).cast("int"),
        ).alias("region"),
        F.col("period_start"),
        F.col("period_end"),
        F.floor(F.rand() * F.lit(256)).cast("int").alias(
            "units_consumed"
        ),
        F.lit(batch_id).cast("long").alias("source_batch"),
        F.current_timestamp().alias("received_at"),
    )
)

readings.write.mode("append").saveAsTable(
    f"{catalog}.{schema}.meter_readings"
)

spark.sql(
    f"""
    SELECT
      source_batch,
      COUNT(*) AS row_count,
      MIN(period_start) AS earliest_period_start,
      MAX(period_end) AS latest_period_end,
      SUM(units_consumed) AS total_units,
      MAX(received_at) AS received_at
    FROM {table_name('meter_readings')}
    WHERE source_batch = {batch_id}
    GROUP BY source_batch
    """
).show(truncate=False)

