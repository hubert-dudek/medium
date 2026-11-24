# Databricks notebook source
from datetime import date, timedelta

base_date = date.fromisoformat(dbutils.widgets.get("base_date"))
prior_week = date.fromisoformat(dbutils.widgets.get("prior_week"))

# COMMAND ----------

assert prior_week == base_date - timedelta(weeks=1)
display(spark.createDataFrame([(base_date, prior_week)], ["job_date", "one_week_earlier"]))
