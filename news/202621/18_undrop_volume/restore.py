# Databricks notebook source
from uuid import UUID, uuid4

dbutils.widgets.text("catalog", spark.catalog.currentCatalog())
catalog = dbutils.widgets.get("catalog").replace("`", "``")
schema = f"`{catalog}`.undrop_demo_{uuid4().hex[:12]}"
volume = f"{schema}.restore_me"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA {schema}")
spark.sql(f"CREATE VOLUME {volume}")
display(spark.sql(f"DESCRIBE VOLUME {volume}"))

# COMMAND ----------

spark.sql(f"DROP VOLUME {volume}")
dropped = spark.sql(f"SHOW VOLUMES DROPPED IN {schema}")
display(dropped)

# COMMAND ----------

volume_id = UUID(str(dropped.first()["volume_id"]))
spark.sql(f"UNDROP VOLUME WITH ID '{volume_id}'")
display(spark.sql(f"DESCRIBE VOLUME {volume}"))

# COMMAND ----------

spark.sql(f"DROP VOLUME IF EXISTS {volume}")
spark.sql(f"DROP SCHEMA IF EXISTS {schema}")
