# Databricks notebook source
from myrest import MyRest

# Register the custom data source with Spark
spark.dataSource.register(MyRest)

default_parallelism = spark.sparkContext.defaultParallelism
print(f"Default parallelism: {default_parallelism}")

df = (
    spark.read
    .format("myrest") 
    .option("startId", "1")
    .option("endId", "100")
    .option("numPartitions", str(default_parallelism * 2))
    .load()
)

num_partitions = df.rdd.getNumPartitions()
print(f"Number of partitions: {num_partitions}")

display(df)

# COMMAND ----------



# COMMAND ----------

# from pyspark.sql.functions import col

# num_cpus = spark.sparkContext.defaultParallelism

# test_data = spark.range(256).select(col("id").alias("userId")).repartition(num_cpus)

# test_data.write.format("myrestdatasource") \
#                .mode("append") \
#                .save()
