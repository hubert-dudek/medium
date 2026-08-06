-- Use case 3
-- Forecasts are authoritative snapshots. Yesterday is preserved, while today and
-- all future dates are replaced by the newest provider issuance on each update.

CREATE STREAMING TABLE weather_forecast_current
TBLPROPERTIES (
  'pipelines.reset.allowed' = 'false'
)
FLOW REPLACE WHERE
  forecast_date >= DATE_SUB(CURRENT_DATE(), CAST(:weather_history_days AS INT))
BY NAME
WITH ranked_forecasts AS (
  SELECT
    location_id,
    forecast_date,
    provider,
    issued_at,
    temperature_c,
    precipitation_pct,
    condition,
    raw_payload,
    ROW_NUMBER() OVER (
      PARTITION BY location_id, forecast_date, provider
      ORDER BY issued_at DESC
    ) AS snapshot_rank
  FROM IDENTIFIER(:source_catalog || '.' || :source_schema || '.weather_forecast_snapshot')
)
SELECT
  location_id,
  forecast_date,
  provider,
  issued_at,
  temperature_c,
  precipitation_pct,
  condition,
  TRY_VARIANT_GET(raw_payload, '$.model', 'STRING') AS model_name,
  TRY_VARIANT_GET(raw_payload, '$.confidence', 'DOUBLE') AS confidence,
  TRY_VARIANT_GET(raw_payload, '$.wind.speed_kmh', 'INT') AS wind_speed_kmh,
  raw_payload
FROM ranked_forecasts
WHERE snapshot_rank = 1;
