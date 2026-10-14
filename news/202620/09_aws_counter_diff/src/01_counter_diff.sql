-- Databricks notebook source
SELECT
  minute,
  requests,
  counter_diff(requests) OVER (ORDER BY minute) AS new_requests
FROM VALUES (1, 100), (2, 160), (3, 205) AS metrics(minute, requests)
ORDER BY minute;

-- COMMAND ----------

SELECT
  minute,
  requests,
  counter_diff(requests) OVER (ORDER BY minute) AS new_requests
FROM VALUES (1, 100), (2, 160), (3, 20), (4, 55) AS metrics(minute, requests)
ORDER BY minute;

-- COMMAND ----------

SELECT
  minute,
  requests,
  counter_diff(requests, started_at) OVER (ORDER BY minute) AS new_requests
FROM VALUES
  (1, 100, TIMESTAMP '2026-10-14 09:00:00'),
  (2, 160, TIMESTAMP '2026-10-14 09:00:00'),
  (3, 200, TIMESTAMP '2026-10-14 09:02:30')
AS metrics(minute, requests, started_at)
ORDER BY minute;
