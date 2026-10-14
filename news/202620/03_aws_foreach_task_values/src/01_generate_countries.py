# Databricks notebook source
countries = ["Poland", "Czechia", "Italy"]
dbutils.jobs.taskValues.set(key="countries", value=countries)

# COMMAND ----------

display(spark.createDataFrame([(country,) for country in countries], ["country"]))
