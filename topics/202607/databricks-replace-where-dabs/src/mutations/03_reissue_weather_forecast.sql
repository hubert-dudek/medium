-- Databricks notebook source
-- Add a later issuance for tomorrow. The pipeline chooses the newest snapshot
-- and replaces today and future dates while preserving any historical snapshot.

USE CATALOG IDENTIFIER(:source_catalog);
USE SCHEMA IDENTIFIER(:source_schema);

DELETE FROM weather_forecast_snapshot
WHERE location_id = 'PRG'
  AND forecast_date = DATE_ADD(CURRENT_DATE(), 1)
  AND provider = 'METEO-DEMO'
  AND TRY_VARIANT_GET(raw_payload, '$.run', 'STRING') = 'demo-correction';

INSERT INTO weather_forecast_snapshot VALUES
  ('PRG', DATE_ADD(CURRENT_DATE(), 1), 'METEO-DEMO', CURRENT_TIMESTAMP(),
   26.50, 85, 'thunderstorms',
   PARSE_JSON('{"model":"wx-v3","run":"demo-correction","confidence":0.94,"wind":{"speed_kmh":38},"alerts":["storm"]}'));

SELECT *
FROM weather_forecast_snapshot
WHERE location_id = 'PRG' AND forecast_date = DATE_ADD(CURRENT_DATE(), 1)
ORDER BY issued_at DESC;
