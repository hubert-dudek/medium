# Databricks notebook source
import json

# COMMAND ----------

spark.sql("DESCRIBE FUNCTION EXTENDED parse_sql").show(truncate=False)

# COMMAND ----------

sql_text = "SELECT customer_id, sum(amount) AS total FROM demo_catalog.sales.orders GROUP BY customer_id"
result = spark.sql("SELECT parse_sql(:sql_text) AS parsed", args={"sql_text": sql_text}).first().parsed
print(json.dumps(json.loads(result), indent=2))
assert json.loads(result)[0]["parse_success"] is True

# COMMAND ----------

return_type = spark.sql("SELECT typeof(parse_sql('SELECT 1')) AS return_type").first().return_type
print(return_type)
assert return_type == "string"

# COMMAND ----------

batch = "SELECT 1 AS first_value; SELEC 2; SELECT 3 AS last_value"
parsed_batch = json.loads(
    spark.sql("SELECT parse_sql(:batch) AS parsed", args={"batch": batch}).first().parsed
)
print(json.dumps(parsed_batch, indent=2))
assert [item["parse_success"] for item in parsed_batch] == [True, False, True]

# COMMAND ----------

sql_text = "SELECT missing_column FROM VALUES (1) AS items(id)"
parsed = json.loads(spark.sql("SELECT parse_sql(:sql_text) AS parsed", args={"sql_text": sql_text}).first().parsed)
assert parsed[0]["parse_success"] is True
print(json.dumps(parsed, indent=2))

try:
    spark.sql(sql_text).collect()
except Exception as error:
    analysis_error = error.getCondition() if hasattr(error, "getCondition") else type(error).__name__
    print(analysis_error)
else:
    raise AssertionError("The intentionally unresolved query unexpectedly executed")

# COMMAND ----------

assert analysis_error.startswith("UNRESOLVED_COLUMN"), analysis_error
dbutils.notebook.exit(json.dumps({"status": "PASS", "return_type": return_type, "batch_parse_success": [item["parse_success"] for item in parsed_batch], "unresolved_query_error": analysis_error}))
