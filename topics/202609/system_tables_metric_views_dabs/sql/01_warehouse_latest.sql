-- Databricks notebook source
-- One current/last-observed name per warehouse; this is an ordinary helper view, not a materialization.
-- The MAP-valued tags column is deliberately excluded from the metric join source.
SET TIME ZONE 'UTC';

-- COMMAND ----------

CREATE OR REPLACE VIEW IDENTIFIER('main.' || :schema || '.warehouse_latest')
COMMENT 'Latest observed warehouse metadata by account, workspace and warehouse. Names are not historical as-of names.'
AS
SELECT account_id, workspace_id, warehouse_id, warehouse_name, warehouse_type,
       change_time AS metadata_updated_at, delete_time
FROM system.compute.warehouses
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY account_id, workspace_id, warehouse_id
  ORDER BY change_time DESC, delete_time DESC NULLS LAST, warehouse_name DESC
) = 1;
