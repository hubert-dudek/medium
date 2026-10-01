# Databricks notebook source
countries = dbutils.jobs.taskValues.get(taskKey="generate_countries", key="countries")
# Use the nested task key to collect every iteration's output as one list.
results = dbutils.jobs.taskValues.get(taskKey="process_country", key="result")
print(results)

for country, result in zip(countries, results):
    print(f"{country}: {result}")
