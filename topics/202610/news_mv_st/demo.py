# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.default.news_20261021_sales
# MAGIC TBLPROPERTIES ('delta.enableRowTracking' = 'true') AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('Berlin', 120), ('Prague', 80),
# MAGIC   ('Berlin', 40), ('Prague', 60)
# MAGIC AS sales(city, amount);

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="workspace.default.news_20261021_revenue")
def city_revenue():
    return (
        spark.read.table("workspace.default.news_20261021_sales")
        .groupBy("city")
        .agg(F.sum("amount").alias("revenue"))
    )

# COMMAND ----------

display(spark.table("workspace.default.news_20261021_revenue").orderBy("city"))
