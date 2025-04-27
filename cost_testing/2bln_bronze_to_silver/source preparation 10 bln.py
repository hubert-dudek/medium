# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tests.bronze_source.transactions (
# MAGIC     transaction_id      BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
# MAGIC     transaction_date    DATE GENERATED ALWAYS AS (CAST(DATE(transaction_time) AS DATE)) NOT NULL,
# MAGIC     transaction_time    TIMESTAMP NOT NULL,
# MAGIC     amount              DECIMAL(10,2),
# MAGIC     tax                 DECIMAL(3,2),
# MAGIC     description         STRING NOT NULL
# MAGIC )
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.appendOnly' = true,
# MAGIC     'delta.enableChangeDataFeed' = true,
# MAGIC     'delta.enableRowTracking' = true
# MAGIC );
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import (
    col, rand, round, current_date, date_sub, expr, lit, md5, concat
)

# Number of records to generate - 2 bln
num_records = 2e9

# Generate a range dataframe [0..(num_records - 1)]
df = spark.range(num_records)

df = (
    df
      .withColumn(
          "transaction_time",
          expr("""
            make_timestamp(
              year(date_sub(current_date(),1)),
              month(date_sub(current_date(),1)),
              day(date_sub(current_date(),CAST(FLOOR(RAND() * 10) + 1 AS INT))),
              cast(rand() * 24 as int),  -- hour: 0-23
              cast(rand() * 60 as int),  -- minute: 0-59
              cast(rand() * 60 as int)   -- second: 0-59
            )
          """)
      )
      .withColumn("random_num", round(rand() * 1000, 2)) 
      .withColumn("amount", col("random_num").cast("decimal(10,2)"))
      .withColumn("tax", lit(0.05).cast("decimal(3,2)"))
      .withColumn("description", md5(concat(col("random_num"))))
      .drop("id", "random_num")
)

df.write.mode("append").saveAsTable("tests.bronze_source.transactions")

spark.sql("OPTIMIZE tests.bronze_source.transactions")
