-- Databricks notebook source
-- MAGIC %md
-- MAGIC PowerBI connection

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![01 powerbi task.png](./01 powerbi task.png "01 powerbi task.png")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC         - task_key: PowerBI_Refresh
-- MAGIC           depends_on:
-- MAGIC             - task_key: MV_Refresh
-- MAGIC           power_bi_task:
-- MAGIC             tables:
-- MAGIC               - name: customers
-- MAGIC                 catalog: main
-- MAGIC                 schema: shop
-- MAGIC                 storage_mode: IMPORT
-- MAGIC             warehouse_id: 7afb4c28fd84d73c
-- MAGIC             power_bi_model:
-- MAGIC               workspace_name: shop
-- MAGIC               model_name: main-shop
-- MAGIC               storage_mode: IMPORT
-- MAGIC               authentication_method: OAUTH
-- MAGIC               overwrite_existing: false
-- MAGIC             connection_resource_name: power-bi
-- MAGIC             refresh_after_update: true

-- COMMAND ----------


