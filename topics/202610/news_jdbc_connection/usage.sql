-- Databricks notebook source
SELECT *
FROM remote_query(
  'my_jdbc',
  query => 'SELECT * FROM sales'
);
