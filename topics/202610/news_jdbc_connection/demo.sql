-- Databricks notebook source
CREATE CONNECTION news_20261021_postgres TYPE JDBC
ENVIRONMENT (
  java_dependencies '["/Volumes/workspace/default/jdbc_drivers/postgresql.jar"]'
)
OPTIONS (
  url 'jdbc:postgresql://demo.invalid:5432/news',
  user 'demo_user',
  password 'demo_password',
  externalOptionsAllowList 'query,dbtable'
);
