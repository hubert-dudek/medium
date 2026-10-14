# Databricks notebook source
from importlib.metadata import version

import emoji

package_version = version("emoji")
assert package_version == "2.14.1"
print(f"emoji {package_version}")

# COMMAND ----------

message = emoji.emojize("Databricks :rocket:")
assert message == "Databricks \U0001f680"
print(message)
