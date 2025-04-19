# Databricks notebook source
# MAGIC %md
# MAGIC https://docs.databricks.com/aws/en/dev-tools/bundles/python

# COMMAND ----------

from databricks.bundles.jobs import Job, Task, NotebookTask


my_jobs_as_code_project_job = Job(
    name="my_jobs_as_code_project_job",
    tasks=[
        Task(
            task_key="notebook_task",
            notebook_task=NotebookTask(
                notebook_path="src/notebook.ipynb",
            ),
        ),
    ],
)
