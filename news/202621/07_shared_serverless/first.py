# Databricks notebook source
from importlib.metadata import version

import humanize

print(version("humanize"), humanize.intcomma(12500))

# COMMAND ----------

first_notebook_value = 12500
print(first_notebook_value)
