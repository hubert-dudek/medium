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

spark.conf.set("spark.sql.ansi.enabled", "true")

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"{quote_identifier(catalog)}.{quote_identifier(schema)}"
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name('meter_usage_summary')} (
      source_batch BIGINT,
      region STRING,
      reading_count BIGINT,
      total_units BIGINT,
      average_units_per_day DOUBLE,
      earliest_period_start DATE,
      latest_period_end DATE,
      processed_at TIMESTAMP
    ) USING DELTA
    """
)

last_processed_batch = spark.sql(
    f"""
    SELECT COALESCE(MAX(source_batch), 0) AS source_batch
    FROM {table_name('meter_usage_summary')}
    """
).first()["source_batch"]

pending = (
    spark.table(f"{catalog}.{schema}.meter_readings")
    .filter(F.col("source_batch") > F.lit(last_processed_batch))
    .withColumn(
        "service_days",
        F.datediff(F.col("period_end"), F.col("period_start")),
    )
    .withColumn(
        "units_per_day",
        F.col("units_consumed").cast("double")
        / F.col("service_days").cast("double"),
    )
)

summary = pending.groupBy("source_batch", "region").agg(
    F.count(F.lit(1)).alias("reading_count"),
    F.sum(F.col("units_consumed")).cast("long").alias("total_units"),
    F.avg(F.col("units_per_day")).alias("average_units_per_day"),
    F.min(F.col("period_start")).alias("earliest_period_start"),
    F.max(F.col("period_end")).alias("latest_period_end"),
).withColumn("processed_at", F.current_timestamp())

summary.write.mode("append").saveAsTable(
    f"{catalog}.{schema}.meter_usage_summary"
)

spark.sql(
    f"""
    SELECT *
    FROM {table_name('meter_usage_summary')}
    ORDER BY source_batch DESC, region
    LIMIT 20
    """
).show(truncate=False)

