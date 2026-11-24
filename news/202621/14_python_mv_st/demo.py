# Databricks notebook source
import re

from pyspark import pipelines as dp
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", spark.catalog.currentCatalog())
catalog = dbutils.widgets.get("catalog")
if not re.fullmatch(r"[a-zA-Z_][a-zA-Z_0-9]*", catalog):
    raise ValueError("Enter a catalog name containing letters, digits or underscores.")

schema = f"{catalog}.news_202621"
sales_table = f"{schema}.mvst_sales"
revenue_view = f"{schema}.mvst_revenue"
streaming_table = f"{schema}.mvst_orders"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {sales_table}
    USING DELTA TBLPROPERTIES ('delta.enableRowTracking' = 'true')
    AS SELECT * FROM VALUES
      (1, 'Berlin', 120), (2, 'Prague', 80),
      (3, 'Berlin', 40), (4, 'Prague', 60)
    AS sales(order_id, city, amount)
""")

# COMMAND ----------

@dp.materialized_view(name=revenue_view)
def city_revenue():
    return spark.read.table(sales_table).groupBy("city").agg(F.sum("amount").alias("revenue"))

# COMMAND ----------

@dp.table(name=streaming_table)
def incoming_orders():
    return spark.readStream.table(sales_table).select("order_id", "city", "amount")

# COMMAND ----------

display(spark.table(revenue_view).orderBy("city"))
display(spark.table(streaming_table).orderBy("order_id"))
