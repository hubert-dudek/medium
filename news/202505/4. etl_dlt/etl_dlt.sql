-- Databricks notebook source
-- Create a streaming table from example_table
CREATE OR REFRESH STREAMING TABLE streaming_table_example AS
SELECT
  *
FROM
  STREAM(hub.default.example_table);

-- Create a temporary view from the streaming table
CREATE TEMPORARY VIEW temp_view_example AS
SELECT
  *
FROM
  streaming_table_example;

-- Create a private materialized view aggregating per amount
CREATE OR REFRESH PRIVATE MATERIALIZED VIEW private_materialized_view_example AS
SELECT
  amount,
  COUNT(*) AS count_per_amount,
  SUM(amount) AS total_amount
FROM
  streaming_table_example
GROUP BY
  amount;
