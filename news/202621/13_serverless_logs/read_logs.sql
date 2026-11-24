-- Databricks notebook source
SELECT
  _metadata.file_name AS log_file,
  value AS log_line
FROM read_files(:run_log_path, format => 'text')
WHERE value LIKE '%NEWS_202621%'
ORDER BY log_file, log_line;
