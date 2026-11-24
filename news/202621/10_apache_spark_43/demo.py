from pathlib import Path

from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[1]").appName("Spark 4.3 JSON_TABLE").getOrCreate()
try:
    if spark.version != "4.3.0":
        raise RuntimeError(f"Expected Apache Spark 4.3.0, found {spark.version}")

    print(f"Apache Spark {spark.version}", flush=True)
    query = Path(__file__).with_name("orders.sql").read_text(encoding="utf-8")
    orders = spark.sql(query)
    orders.show()
    assert [tuple(row) for row in orders.collect()] == [(1, "tea", 2), (2, "coffee", 1)]
finally:
    spark.stop()
