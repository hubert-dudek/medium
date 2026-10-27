# Databricks notebook source
sales = spark.table(
    "samples.bakehouse.sales_transactions"
).limit(1000)
df = sales.groupBy("product").count()
df.cache()

# COMMAND ----------

display(df.orderBy("count", ascending=False))

# COMMAND ----------

print(df.storageLevel)
df.explain()

# COMMAND ----------

df.unpersist()
