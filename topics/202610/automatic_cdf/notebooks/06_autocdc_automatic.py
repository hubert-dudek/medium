# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

# COMMAND ----------

@dp.temporary_view(name="source_changes")
def source_changes():
    return (
        spark.readStream.option("readChangeFeed", "true")
        .table("main.automatic_cdf_20261010.demo_storage_automatic")
        .where(col("_change_type") != "update_preimage")
    )

# COMMAND ----------

dp.create_streaming_table(name="demo_storage_automatic_target")
dp.create_auto_cdc_flow(
    name="apply_source_changes",
    target="demo_storage_automatic_target",
    source="source_changes",
    keys=["id"],
    sequence_by=col("_commit_version"),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],
    stored_as_scd_type=1,
)
