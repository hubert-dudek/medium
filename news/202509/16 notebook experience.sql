-- Databricks notebook source
-- MAGIC %md
-- MAGIC The following improvements have been made to the notebook editing experience:
-- MAGIC
-- MAGIC     Add a split view to edit notebooks side by side.
-- MAGIC     Pressing Cmd + F (Mac) or Ctrl + F (Windows) in a notebook now opens the native Databricks find-and-replace tool. This allows you to quickly search and replace text throughout your entire notebook, including content outside the current viewport. See Find and replace text.
-- MAGIC     Quickly switch between tab groups based on authoring contexts using the Home icon., Query editor icon., and Pipeline icon. icons on the top left in the editor. See Switch between authoring contexts.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Use the cell execution minimap to track your notebook’s progress at a glance. The minimap appears in the right margin and shows each cell’s execution state (skipped, queued, running, success, or error). Hover to see cell details, or click to jump directly to a cell.
-- MAGIC
-- MAGIC For information about using the cell execution minimap, see Navigate the Databricks notebook and file editor.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC https://docs.databricks.com/aws/en/notebooks/notebook-editor

-- COMMAND ----------

-- DBTITLE 1,orders
SELECT * FROM default.orders;

-- COMMAND ----------

-- DBTITLE 1,cities
SELECT * FROM default.cities;

-- COMMAND ----------

-- DBTITLE 1,cached query
SELECT * FROMsdds default.orders;

-- COMMAND ----------

-- DBTITLE 1,contacts
SELECT * FROM default.contacts;

-- COMMAND ----------

-- DBTITLE 1,cached query
SELECT * FROM default.orders;
