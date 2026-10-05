# Databricks notebook source
import json

from pyspark.sql import functions as F

queries = [
    ("cte", "WITH recent_orders AS (SELECT * FROM demo_catalog.sales.orders WHERE amount > 100) SELECT r.id, c.name FROM recent_orders r JOIN demo_catalog.sales.customers c ON r.customer_id = c.id"),
    ("merge", "MERGE INTO demo_catalog.gold.orders t USING demo_catalog.silver.orders s ON t.id = s.id WHEN MATCHED THEN UPDATE SET amount = s.amount WHEN NOT MATCHED THEN INSERT *"),
    ("view", "CREATE OR REPLACE VIEW demo_catalog.gold.large_orders AS SELECT id, amount FROM demo_catalog.silver.orders WHERE amount > 1000"),
    ("quoted_name", "SELECT `order.id` FROM `demo_catalog`.`sales`.`orders.archive`"),
    ("expressions", "SELECT o.id, o.amount + 1, o.amount + 1 AS adjusted_amount, o.* FROM demo_catalog.sales.orders o"),
    ("parameters", "SELECT * FROM demo_catalog.sales.orders WHERE country = :country AND amount > :minimum_amount"),
]
inputs = spark.createDataFrame(queries, "case_name STRING, sql_text STRING")
raw = inputs.withColumn("parsed_json", F.expr("parse_sql(sql_text)"))
display(raw)

# COMMAND ----------

schema = "ARRAY<STRUCT<start:INT,length:INT,parse_success:BOOLEAN,statement_identifier:STRING,statement_code:INT,target_table_references:ARRAY<ARRAY<STRING>>,source_table_references:ARRAY<ARRAY<STRING>>,function_references:ARRAY<ARRAY<STRING>>,select_list:ARRAY<STRUCT<name:ARRAY<STRING>>>,parameter_markers:STRUCT<named:ARRAY<STRING>,unnamed_count:INT>>>"
statements = raw.select("case_name", F.explode(F.from_json("parsed_json", schema)).alias("statement"))
display(statements.select("case_name", "statement.*"))

# COMMAND ----------

sources = statements.select("case_name", F.lit("source").alias("role"), F.explode("statement.source_table_references").alias("identifier_parts"))
targets = statements.select("case_name", F.lit("target").alias("role"), F.explode("statement.target_table_references").alias("identifier_parts"))
references = sources.unionByName(targets)
display(references.orderBy("case_name", "role"))

# COMMAND ----------

metadata = {row.case_name: json.loads(row.parsed_json)[0] for row in raw.collect()}
assert all(item["parse_success"] for item in metadata.values())
assert {tuple(parts) for parts in metadata["cte"]["source_table_references"]} == {
    ("demo_catalog", "sales", "orders"),
    ("demo_catalog", "sales", "customers"),
}
assert metadata["merge"]["target_table_references"] == [["demo_catalog", "gold", "orders"]]
assert metadata["merge"]["source_table_references"] == [["demo_catalog", "silver", "orders"]]
assert metadata["quoted_name"]["source_table_references"] == [["demo_catalog", "sales", "orders.archive"]]
assert set(metadata["parameters"]["parameter_markers"]["named"]) == {"country", "minimum_amount"}
dbutils.notebook.exit(json.dumps({"status": "PASS", "queries": len(queries), "metadata": metadata}))
