# Databricks notebook source
dbutils.widgets.text("country", "")
print(dbutils.widgets.get("country"))
