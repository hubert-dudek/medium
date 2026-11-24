# Databricks notebook source
from importlib.metadata import version

import humanize

print(version("humanize"), humanize.intcomma(25000))

# COMMAND ----------

print("first_notebook_value" in globals())
