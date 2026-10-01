# Databricks notebook source
# Each iteration publishes the same hardcoded result for this demo.
dbutils.jobs.taskValues.set(key="result", value="done")
