# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Demo: Instance Pool Job
# This notebook runs on a cluster backed by an instance pool
print(f"Spark version: {spark.version}")
print(f"Number of workerssssss: {spark.sparkContext.defaultParallelism}")
print("Instance pool demo completed successfully!")

# COMMAND ----------


