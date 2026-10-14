# Databricks notebook source
from pyspark import pipelines as dp

source_table = spark.conf.get("demo.source_table")
state_partitions = spark.conf.get("demo.state_partitions")

# COMMAND ----------

@dp.table(
    name="pipeline_unique_events",
    spark_conf={
        "spark.sql.streaming.stateStore.partitions": state_partitions
    },
)
def pipeline_unique_events():
    return (
        spark.readStream.table(source_table)
        .dropDuplicates(["event_id"])
    )
