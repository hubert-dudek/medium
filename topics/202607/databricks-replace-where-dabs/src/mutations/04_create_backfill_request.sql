-- Databricks notebook source
-- Correct a historical source range and enqueue exactly that range for the
-- monitor. The monitor converts these dates into pipeline parameter overrides.

USE CATALOG IDENTIFIER(:source_catalog);
USE SCHEMA IDENTIFIER(:source_schema);

UPDATE customer_event_raw
SET amount = CASE
      WHEN event_type = 'purchase' THEN CAST(ROUND((20 + (((event_id - 1) * 13) % 180)) * 1.10, 2) AS DECIMAL(18, 2))
      WHEN event_type = 'refund' THEN CAST(-5 - (((event_id - 1) * 7) % 40) AS DECIMAL(18, 2))
      ELSE CAST(0 AS DECIMAL(18, 2))
    END,
    quality_score = LEAST(
      CAST(1.00 AS DECIMAL(5, 2)),
      CAST(0.73 + (((event_id - 1) % 30) / 100.0) AS DECIMAL(5, 2))
    ),
    source_updated_at = CURRENT_TIMESTAMP()
WHERE event_date BETWEEN DATE_SUB(CURRENT_DATE(), 45) AND DATE_SUB(CURRENT_DATE(), 38);

DELETE FROM backfill_requests
WHERE request_id = 'BF-DEMO-001';

INSERT INTO backfill_requests (
  request_id, start_date, end_date, reason, status, requested_at,
  started_at, completed_at, pipeline_update_id, claim_token, error_message
)
VALUES (
  'BF-DEMO-001',
  DATE_SUB(CURRENT_DATE(), 45),
  DATE_SUB(CURRENT_DATE(), 38),
  'Recompute after correcting purchase amounts and quality scores',
  'PENDING',
  CURRENT_TIMESTAMP(),
  NULL, NULL, NULL, NULL, NULL
);

SELECT *
FROM backfill_requests
ORDER BY requested_at DESC;
