# Databricks notebook source
# MAGIC %md
# MAGIC # Environments on classic compute
# MAGIC
# MAGIC Open the existing single-node Runtime 19 compute. In Advanced > Dependencies, show Environments mode and the 10-minute idle timeout. This mode requires Runtime 19 or later and standard access. It supports Python workloads; Scala, R, JAR tasks, and cluster-scoped libraries are not supported.

# COMMAND ----------

# MAGIC %md
# MAGIC Attach `01_dependency_check` to that compute. Open the Environment side pane, choose Custom, and select this chapter's `environment.yml`. Apply the environment, then run the two cells.
# MAGIC
# MAGIC Show `emoji 2.14.1` and the rocket result. The job uses the same YAML file through `base_environment`.

# COMMAND ----------

# MAGIC %md
# MAGIC From PowerShell 7, run `pwsh -File .\run_demo.ps1` in this chapter folder after deploying the bundle. The wrapper uses the Azure CLI profile, waits for a bounded run, and terminates compute in `finally` when it started that compute.
# MAGIC
# MAGIC If compute was already running, the wrapper leaves it running. Stop it immediately after recording when it is no longer needed.
