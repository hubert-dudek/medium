# Databricks notebook source
countries = ["Poland", "Czechia", "Italy"]
dbutils.jobs.taskValues.set(key="countries", value=countries)
print(countries)
