-- Use case 4
-- The normal transformation and its bounded replacement window are reusable.
-- A monitor reads requested ranges and starts this pipeline with per-update
-- backfill_start_date and backfill_end_date parameter overrides.

CREATE STREAMING TABLE customer_daily_metrics
TBLPROPERTIES (
  'pipelines.reset.allowed' = 'false'
)
FLOW REPLACE WHERE
  metric_date >= CAST(:backfill_start_date AS DATE)
  AND metric_date <= CAST(:backfill_end_date AS DATE)
BY NAME
SELECT
  event_date AS metric_date,
  customer_id,
  COUNT(*) AS event_count,
  COUNT_IF(event_type = 'purchase') AS purchase_count,
  COUNT_IF(event_type = 'refund') AS refund_count,
  CAST(SUM(CASE WHEN event_type IN ('purchase', 'refund') THEN amount ELSE 0 END) AS DECIMAL(18, 2)) AS net_revenue,
  COUNT_IF(quality_score >= 0.90) AS high_quality_event_count,
  CAST(AVG(quality_score) AS DECIMAL(8, 4)) AS average_quality_score,
  MAX(source_updated_at) AS last_source_update
FROM IDENTIFIER(:source_catalog || '.' || :source_schema || '.customer_event_raw')
GROUP BY event_date, customer_id;
