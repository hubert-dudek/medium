# Databricks notebook source
from pyspark.sql import functions as F

sales = spark.range(1000).select((F.col("id") % 3).alias("product"))
counts = sales.groupBy("product").count()

# COMMAND ----------

counts.cache()
display(counts.orderBy("product"))

# COMMAND ----------

print(counts.storageLevel)
counts.explain()

# COMMAND ----------

counts.unpersist()
