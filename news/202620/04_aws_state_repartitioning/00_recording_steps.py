# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Resize streaming state
# MAGIC **Workspace:** aws-serverless-dev · **Compute:** serverless pipeline; environment 6 for the standalone notebook.
# MAGIC
# MAGIC Show `src/01_stateful_pipeline.py`, then the partition setting in `databricks.yml`, then the real `event_log` results in `src/04_results.sql`.
# MAGIC The pipeline's state partitions change **2 → 4 → 2**, retaining the same checkpoint. Output table partitioning is a different concept.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline demonstration
# MAGIC From this chapter's directory, with `DATABRICKS_CONFIG_FILE=C:\git\cli\.databrickscfg`:
# MAGIC ```powershell
# MAGIC & C:\git\cli\databricks.exe bundle deploy --profile DEFAULT
# MAGIC py .\run_pipeline_demo.py --warehouse-id 8a19c545d43c9bc3 --profile DEFAULT --cli C:\git\cli\databricks.exe
# MAGIC ```
# MAGIC The controller appends three small batches, deploys each partition count, performs ordinary updates, and checks state metrics from `event_log`. No full refresh is used.
# MAGIC Each repeat adds 16 new unique event IDs; existing rows and checkpoint state are retained. The results notebook shows all recorded phases. For the recording, the completed run can be shown without rerunning it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standalone demonstration
# MAGIC Open `src/02_streaming_repartition.py` and run its cells in order on serverless environment 6, or run the bundle job:
# MAGIC ```powershell
# MAGIC & C:\git\cli\databricks.exe bundle run standalone --profile DEFAULT
# MAGIC ```
# MAGIC Three bounded AvailableNow runs use the same checkpoint, with cumulative counts **20 → 40 → 60**. A new notebook execution creates its own uniquely named small source, target and checkpoint.
# MAGIC Explain the stop/change/restart sequence: this is not repartitioning a still-running query in place.
# MAGIC
# MAGIC [State repartitioning documentation](https://docs.databricks.com/aws/en/structured-streaming/state-repartitioning)
