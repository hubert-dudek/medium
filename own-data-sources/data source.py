# Databricks notebook source
from myrestdatasource import MyRestDataSource

# Register the custom data source with Spark
spark.dataSource.register(MyRestDataSource)

# Read from the data source
df = (
    spark.read
         .format("myrestdatasource")
         .option("endpoint", "posts")  # url /posts/
         .load()
)

df.show(5)

# (Optional) Write to the data source
test_data = spark.createDataFrame(
    [(999, 12345, "Test Title", "This is a test body.")],
    ["userId", "id", "title", "body"]
)

test_data.write.format("myrestdatasource") \
               .option("endpoint", "posts") \
               .option("method", "POST") \
               .mode("append") \
               .save()
