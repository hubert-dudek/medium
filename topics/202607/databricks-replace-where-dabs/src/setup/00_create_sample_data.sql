-- Databricks notebook source
-- Recreates all sample source tables. Run through the setup_sample_data job.

USE CATALOG IDENTIFIER(:source_catalog);
USE SCHEMA IDENTIFIER(:source_schema);

-- COMMAND ----------
-- USE CASE 1: Orders joined to a current, semi-structured product specification.

CREATE OR REPLACE TABLE orders_fact (
  order_id          BIGINT,
  order_date        DATE,
  product_id        INT,
  customer_id       INT,
  quantity          INT,
  unit_price        DECIMAL(12, 2),
  source_updated_at TIMESTAMP
)
USING DELTA;

INSERT INTO orders_fact
SELECT
  id + 1 AS order_id,
  DATE_SUB(CURRENT_DATE(), CAST(id % 60 AS INT)) AS order_date,
  CAST((id % 4) + 1 AS INT) AS product_id,
  CAST((id % 75) + 1000 AS INT) AS customer_id,
  CAST((id % 5) + 1 AS INT) AS quantity,
  CAST(25 + ((id * 17) % 175) AS DECIMAL(12, 2)) AS unit_price,
  CURRENT_TIMESTAMP() AS source_updated_at
FROM RANGE(0, 600);

CREATE OR REPLACE TABLE product_spec_dim (
  product_id   INT,
  product_name STRING,
  product_spec VARIANT,
  spec_version INT,
  updated_at   TIMESTAMP
)
USING DELTA;

INSERT INTO product_spec_dim VALUES
  (1, 'LakeBook Air',
   PARSE_JSON('{"family":"laptop","tier":"standard","hardware":{"ram_gb":16,"storage_gb":512},"energy":{"class":"A"},"connectivity":["wifi6","usb-c"]}'),
   1, CURRENT_TIMESTAMP()),
  (2, 'LakeBook Pro',
   PARSE_JSON('{"family":"laptop","tier":"premium","hardware":{"ram_gb":32,"storage_gb":1024},"energy":{"class":"B"},"connectivity":["wifi7","usb-c","thunderbolt"]}'),
   1, CURRENT_TIMESTAMP()),
  (3, 'DeltaPhone',
   PARSE_JSON('{"family":"phone","tier":"premium","hardware":{"ram_gb":12,"storage_gb":512},"energy":{"class":"A"},"connectivity":["5g","wifi7"]}'),
   1, CURRENT_TIMESTAMP()),
  (4, 'StreamWatch',
   PARSE_JSON('{"family":"wearable","tier":"standard","hardware":{"ram_gb":2,"storage_gb":64},"energy":{"class":"A+"},"connectivity":["lte","bluetooth"]}'),
   1, CURRENT_TIMESTAMP());

-- COMMAND ----------
-- USE CASE 2: Effective-dated VAT rules.

CREATE OR REPLACE TABLE invoice_lines (
  invoice_line_id BIGINT,
  invoice_id      STRING,
  invoice_date    DATE,
  country_code    STRING,
  product_type    STRING,
  net_amount      DECIMAL(18, 2),
  source_updated_at TIMESTAMP
)
USING DELTA;

INSERT INTO invoice_lines VALUES
  (1, 'INV-2025-001', DATE '2025-12-15', 'CZ', 'STANDARD', 1000.00, CURRENT_TIMESTAMP()),
  (2, 'INV-2025-002', DATE '2025-12-28', 'CZ', 'DIGITAL',   400.00, CURRENT_TIMESTAMP()),
  (3, 'INV-2026-001', DATE '2026-01-03', 'CZ', 'STANDARD', 1200.00, CURRENT_TIMESTAMP()),
  (4, 'INV-2026-002', DATE '2026-01-12', 'CZ', 'DIGITAL',   600.00, CURRENT_TIMESTAMP()),
  (5, 'INV-2026-003', DATE '2026-02-15', 'CZ', 'STANDARD',  850.00, CURRENT_TIMESTAMP()),
  (6, 'INV-2026-004', DATE '2026-07-18', 'CZ', 'STANDARD', 2300.00, CURRENT_TIMESTAMP()),
  (7, 'INV-2026-005', DATE '2026-07-20', 'PL', 'STANDARD', 1700.00, CURRENT_TIMESTAMP());

CREATE OR REPLACE TABLE vat_rate_dim (
  country_code STRING,
  product_type STRING,
  valid_from   DATE,
  valid_to     DATE,
  vat_rate     DECIMAL(8, 4),
  rule_version STRING,
  updated_at   TIMESTAMP
)
USING DELTA;

INSERT INTO vat_rate_dim VALUES
  ('CZ', 'STANDARD', DATE '1900-01-01', DATE '2026-01-01', 0.2100, 'CZ-STD-2025', CURRENT_TIMESTAMP()),
  ('CZ', 'STANDARD', DATE '2026-01-01', NULL,              0.2300, 'CZ-STD-2026-v1', CURRENT_TIMESTAMP()),
  ('CZ', 'DIGITAL',  DATE '1900-01-01', NULL,              0.1200, 'CZ-DIGITAL', CURRENT_TIMESTAMP()),
  ('PL', 'STANDARD', DATE '1900-01-01', NULL,              0.2300, 'PL-STD', CURRENT_TIMESTAMP());

-- COMMAND ----------
-- USE CASE 3: Multiple snapshots of weather forecasts.

CREATE OR REPLACE TABLE weather_forecast_snapshot (
  location_id       STRING,
  forecast_date     DATE,
  provider          STRING,
  issued_at         TIMESTAMP,
  temperature_c     DECIMAL(6, 2),
  precipitation_pct INT,
  condition         STRING,
  raw_payload       VARIANT
)
USING DELTA;

INSERT INTO weather_forecast_snapshot
SELECT
  location_id,
  DATE_ADD(CURRENT_DATE(), day_offset) AS forecast_date,
  'METEO-DEMO' AS provider,
  CURRENT_TIMESTAMP() - INTERVAL 12 HOURS AS issued_at,
  CAST(base_temperature + day_offset AS DECIMAL(6, 2)) AS temperature_c,
  CAST(20 + day_offset * 5 AS INT) AS precipitation_pct,
  CASE WHEN day_offset % 3 = 0 THEN 'rain' ELSE 'partly_cloudy' END AS condition,
  PARSE_JSON(CONCAT(
    '{"model":"wx-v1","run":"morning","confidence":',
    CAST(0.70 + day_offset * 0.02 AS STRING),
    ',"wind":{"speed_kmh":', CAST(12 + day_offset AS STRING), '}}'
  )) AS raw_payload
FROM VALUES
  ('PRG', 11.0),
  ('WAW', 13.0)
AS locations(location_id, base_temperature)
CROSS JOIN (
  SELECT EXPLODE(SEQUENCE(-1, 5)) AS day_offset
) AS offsets;

INSERT INTO weather_forecast_snapshot
SELECT
  location_id,
  DATE_ADD(CURRENT_DATE(), day_offset) AS forecast_date,
  'METEO-DEMO' AS provider,
  CURRENT_TIMESTAMP() - INTERVAL 2 HOURS AS issued_at,
  CAST(base_temperature + day_offset + 1.5 AS DECIMAL(6, 2)) AS temperature_c,
  CAST(10 + day_offset * 6 AS INT) AS precipitation_pct,
  CASE WHEN day_offset % 2 = 0 THEN 'sunny' ELSE 'cloudy' END AS condition,
  PARSE_JSON(CONCAT(
    '{"model":"wx-v2","run":"evening","confidence":',
    CAST(0.82 + day_offset * 0.01 AS STRING),
    ',"wind":{"speed_kmh":', CAST(9 + day_offset AS STRING), '}}'
  )) AS raw_payload
FROM VALUES
  ('PRG', 11.0),
  ('WAW', 13.0)
AS locations(location_id, base_temperature)
CROSS JOIN (
  SELECT EXPLODE(SEQUENCE(-1, 5)) AS day_offset
) AS offsets;

-- COMMAND ----------
-- USE CASE 4: Event facts plus a request table consumed by the backfill monitor.

CREATE OR REPLACE TABLE customer_event_raw (
  event_id          BIGINT,
  event_date        DATE,
  customer_id       INT,
  event_type        STRING,
  amount             DECIMAL(18, 2),
  quality_score      DECIMAL(5, 2),
  source_updated_at  TIMESTAMP
)
USING DELTA;

INSERT INTO customer_event_raw
SELECT
  id + 1 AS event_id,
  DATE_SUB(CURRENT_DATE(), CAST(id % 120 AS INT)) AS event_date,
  CAST((id % 25) + 1 AS INT) AS customer_id,
  CASE
    WHEN id % 5 = 0 THEN 'purchase'
    WHEN id % 5 = 1 THEN 'refund'
    WHEN id % 5 = 2 THEN 'login'
    ELSE 'browse'
  END AS event_type,
  CASE
    WHEN id % 5 = 0 THEN CAST(20 + ((id * 13) % 180) AS DECIMAL(18, 2))
    WHEN id % 5 = 1 THEN CAST(-5 - ((id * 7) % 40) AS DECIMAL(18, 2))
    ELSE CAST(0 AS DECIMAL(18, 2))
  END AS amount,
  CAST(0.70 + ((id % 30) / 100.0) AS DECIMAL(5, 2)) AS quality_score,
  CURRENT_TIMESTAMP() AS source_updated_at
FROM RANGE(0, 1200);

CREATE OR REPLACE TABLE backfill_requests (
  request_id         STRING,
  start_date         DATE,
  end_date           DATE,
  reason             STRING,
  status             STRING,
  requested_at       TIMESTAMP,
  started_at         TIMESTAMP,
  completed_at       TIMESTAMP,
  pipeline_update_id STRING,
  claim_token        STRING,
  error_message      STRING
)
USING DELTA;

-- No pending request is inserted by setup. The demo mutation creates one.

-- COMMAND ----------
-- USE CASE 5: Open-period accounting enrichment with semi-structured allocations.

CREATE OR REPLACE TABLE invoice_event (
  legal_entity      STRING,
  accounting_date   DATE,
  invoice_id        STRING,
  amount_local      DECIMAL(18, 2),
  currency          STRING,
  allocation_spec   VARIANT,
  source_updated_at TIMESTAMP
)
USING DELTA;

INSERT INTO invoice_event VALUES
  ('PL01', DATE '2026-06-28', 'ACC-100', 1000.00, 'PLN',
   PARSE_JSON('{"cost_center":"CONSULTING","commission_pct":0.05,"sales_region":"CENTRAL","allocation_version":1}'),
   CURRENT_TIMESTAMP()),
  ('PL01', DATE '2026-07-10', 'ACC-101', 2400.00, 'PLN',
   PARSE_JSON('{"cost_center":"TRAINING","commission_pct":0.08,"sales_region":"SOUTH","allocation_version":1}'),
   CURRENT_TIMESTAMP()),
  ('PL01', DATE '2026-07-22', 'ACC-102', 1800.00, 'PLN',
   PARSE_JSON('{"cost_center":"SUBSCRIPTION","commission_pct":0.03,"sales_region":"NORTH","allocation_version":1}'),
   CURRENT_TIMESTAMP()),
  ('PL01', DATE '2026-08-04', 'ACC-103', 3200.00, 'PLN',
   PARSE_JSON('{"cost_center":"CONSULTING","commission_pct":0.06,"sales_region":"WEST","allocation_version":1}'),
   CURRENT_TIMESTAMP()),
  ('CZ01', DATE '2026-07-12', 'ACC-200', 4200.00, 'CZK',
   PARSE_JSON('{"cost_center":"TRAINING","commission_pct":0.07,"sales_region":"MORAVIA","allocation_version":1}'),
   CURRENT_TIMESTAMP());

CREATE OR REPLACE TABLE daily_fx (
  rate_date   DATE,
  currency    STRING,
  rate_to_eur DECIMAL(18, 6),
  rate_version STRING
)
USING DELTA;

INSERT INTO daily_fx VALUES
  (DATE '2026-06-28', 'PLN', 0.232000, 'ECB-2026-06-28'),
  (DATE '2026-07-10', 'PLN', 0.234000, 'ECB-2026-07-10'),
  (DATE '2026-07-22', 'PLN', 0.233000, 'ECB-2026-07-22'),
  (DATE '2026-08-04', 'PLN', 0.235000, 'ECB-2026-08-04'),
  (DATE '2026-07-12', 'CZK', 0.040000, 'ECB-2026-07-12');

CREATE OR REPLACE TABLE period_control (
  legal_entity STRING,
  period_start DATE,
  period_end   DATE,
  status       STRING,
  changed_at   TIMESTAMP
)
USING DELTA;

INSERT INTO period_control VALUES
  ('PL01', DATE '2026-06-01', DATE '2026-06-30', 'CLOSED', CURRENT_TIMESTAMP()),
  ('PL01', DATE '2026-07-01', DATE '2026-07-31', 'OPEN', CURRENT_TIMESTAMP()),
  ('PL01', DATE '2026-08-01', DATE '2026-08-31', 'FUTURE', CURRENT_TIMESTAMP()),
  ('CZ01', DATE '2026-07-01', DATE '2026-07-31', 'OPEN', CURRENT_TIMESTAMP());

-- COMMAND ----------
-- USE CASE 6: Versioned provider manifests and their authoritative daily rows.

CREATE OR REPLACE TABLE settlement_manifest (
  provider      STRING,
  business_date DATE,
  file_version  INT,
  accepted_at   TIMESTAMP,
  status        STRING,
  file_checksum STRING
)
USING DELTA;

INSERT INTO settlement_manifest VALUES
  ('PAYFAST', DATE_SUB(CURRENT_DATE(), 2), 1, CURRENT_TIMESTAMP() - INTERVAL 1 DAY, 'ACCEPTED', 'payfast-d2-v1'),
  ('PAYFAST', DATE_SUB(CURRENT_DATE(), 1), 1, CURRENT_TIMESTAMP() - INTERVAL 6 HOURS, 'ACCEPTED', 'payfast-d1-v1'),
  ('CARDNET', DATE_SUB(CURRENT_DATE(), 2), 1, CURRENT_TIMESTAMP() - INTERVAL 1 DAY, 'ACCEPTED', 'cardnet-d2-v1');

CREATE OR REPLACE TABLE settlement_rows (
  provider       STRING,
  business_date  DATE,
  file_version   INT,
  transaction_id STRING,
  gross_amount   DECIMAL(18, 2),
  fee_amount     DECIMAL(18, 2),
  attributes     VARIANT
)
USING DELTA;

INSERT INTO settlement_rows VALUES
  ('PAYFAST', DATE_SUB(CURRENT_DATE(), 2), 1, 'T100', 100.00, 2.50,
   PARSE_JSON('{"status":"captured","card_country":"PL"}')),
  ('PAYFAST', DATE_SUB(CURRENT_DATE(), 2), 1, 'T200',  50.00, 1.25,
   PARSE_JSON('{"status":"captured","card_country":"CZ","duplicate_candidate":true}')),
  ('PAYFAST', DATE_SUB(CURRENT_DATE(), 1), 1, 'T300', 220.00, 4.40,
   PARSE_JSON('{"status":"captured","card_country":"DE"}')),
  ('CARDNET', DATE_SUB(CURRENT_DATE(), 2), 1, 'C100', 300.00, 6.00,
   PARSE_JSON('{"status":"captured","card_country":"PL"}'));

-- COMMAND ----------
SELECT
  CURRENT_CATALOG() AS source_catalog,
  CURRENT_SCHEMA() AS source_schema,
  'Sample source data recreated successfully' AS message;
