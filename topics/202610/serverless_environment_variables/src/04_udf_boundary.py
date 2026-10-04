# Databricks notebook source
# MAGIC %md
# MAGIC # 4. Check the Spark boundary
# MAGIC Select `app_config` for this task. Its process gets the marker; the Spark UDF does not.

# COMMAND ----------
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
print(result)
assert result["task_process"] == "serverless-env-demo"
assert result["spark_udf"] is None

# COMMAND ----------
import json

dbutils.notebook.exit(json.dumps(result))
