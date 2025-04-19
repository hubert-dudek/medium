# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TEMPORARY VIEW my_view (sales_day, total_sales, sales_rep)
# MAGIC   AS SELECT date(sales_date) AS sale_day, SUM(sales) AS total_sales, FIRST(sales_rep) FROM sales GROUP BY date(sales_date), sales_rep;
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE sales_by_date
# MAGIC   AS SELECT * FROM STREAM my_view;
