# Databricks notebook source
df = spark.range(1000).selectExpr("id % 3 AS group_id")
df.cache()

# COMMAND ----------

display(df.groupBy("group_id").count().orderBy("group_id"))

# COMMAND ----------

print(df.storageLevel)
df.explain()

# COMMAND ----------

df.unpersist()
