# Databricks notebook source
RELEASE = "B"
MULTIPLIER = -1

# COMMAND ----------

# An isolated three-row fixture. No files, widgets, or persistent data writes.
result = spark.sql(
    """
    SELECT id + 1 AS item_id,
           (id + 1) * 10 * :multiplier AS adjusted_amount
    FROM range(3)
    """,
    args={"multiplier": MULTIPLIER},
)
actual = [row.adjusted_amount for row in result.orderBy("item_id").collect()]
print(f"Release {RELEASE}; adjusted amounts {actual}")

if any(amount <= 0 for amount in actual):
    raise RuntimeError(
        f"VALIDATION FAILED: release {RELEASE}; adjusted amounts {actual} "
        "must all be positive"
    )

display(result.orderBy("item_id"))
print(f"VALIDATION PASSED: release {RELEASE}; adjusted amounts {actual}")
