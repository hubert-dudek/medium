-- Databricks notebook source
CREATE CONNECTION news_202621_jdbc TYPE JDBC
ENVIRONMENT (
  java_dependencies '["/Volumes/<catalog>/<schema>/<volume>/<driver>.jar"]'
)
OPTIONS (
  url '<JDBC URL without credentials>',
  user secret('<secret-scope>', '<username-key>'),
  password secret('<secret-scope>', '<password-key>'),
  externalOptionsAllowList 'query'
);
