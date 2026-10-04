# Databricks notebook source
# MAGIC %md
# MAGIC # Task process versus Spark UDF
# MAGIC The UDF reads its own process environment. It does not capture a driver value.

# COMMAND ----------
import json
import os
from pyspark.sql import functions as F
from pyspark.sql import types as T

@F.udf(T.StringType())
def read_marker_in_udf():
    import os
    return os.getenv("ARTICLE_ENV_MARKER")

result = {
    "task_process": os.getenv("ARTICLE_ENV_MARKER"),
    "spark_udf": spark.range(1).select(read_marker_in_udf().alias("value")).first()["value"],
}
print(json.dumps(result, indent=2))
assert result["task_process"] == "serverless-env-demo"
assert result["spark_udf"] is None, "UDF scope differs from the documented behavior"

dbutils.notebook.exit(json.dumps(result))
