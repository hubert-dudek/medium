# Databricks notebook source
from myrest import MyRest

# Register the custom data source with Spark
spark.dataSource.register(MyRest)

# Read from the data source
df = (
    spark.read
         .format("myrest")
         .load()
)

display(df)

# COMMAND ----------

# from pyspark.sql.functions import col

# num_cpus = spark.sparkContext.defaultParallelism

# test_data = spark.range(256).select(col("id").alias("userId")).repartition(num_cpus)

# test_data.write.format("myrestdatasource") \
#                .mode("append") \
#                .save()
