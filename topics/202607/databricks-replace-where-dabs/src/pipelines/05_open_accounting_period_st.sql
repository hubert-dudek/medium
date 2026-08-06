-- Use case 5
-- The replace boundary follows a business-controlled accounting state rather
-- than a generic rolling number of days. Closed history remains unchanged.

CREATE STREAMING TABLE invoice_reporting_open_period
TBLPROPERTIES (
  'pipelines.reset.allowed' = 'false'
)
FLOW REPLACE WHERE
  legal_entity = :legal_entity
  AND accounting_date >= CAST(:open_period_start AS DATE)
BY NAME
SELECT
  i.legal_entity,
  i.accounting_date,
  i.invoice_id,
  TRY_VARIANT_GET(i.allocation_spec, '$.cost_center', 'STRING') AS cost_center,
  TRY_VARIANT_GET(i.allocation_spec, '$.sales_region', 'STRING') AS sales_region,
  TRY_VARIANT_GET(i.allocation_spec, '$.allocation_version', 'INT') AS allocation_version,
  TRY_VARIANT_GET(i.allocation_spec, '$.commission_pct', 'DOUBLE') AS commission_pct,
  i.amount_local,
  i.currency,
  fx.rate_to_eur,
  fx.rate_version,
  CAST(ROUND(i.amount_local * fx.rate_to_eur, 2) AS DECIMAL(18, 2)) AS amount_eur,
  CAST(
    ROUND(
      i.amount_local
      * fx.rate_to_eur
      * COALESCE(TRY_VARIANT_GET(i.allocation_spec, '$.commission_pct', 'DOUBLE'), 0.0),
      2
    ) AS DECIMAL(18, 2)
  ) AS commission_eur,
  i.allocation_spec,
  i.source_updated_at
FROM IDENTIFIER(:source_catalog || '.' || :source_schema || '.invoice_event') AS i
JOIN IDENTIFIER(:source_catalog || '.' || :source_schema || '.daily_fx') AS fx
  ON i.accounting_date = fx.rate_date
 AND i.currency = fx.currency
WHERE i.legal_entity = :legal_entity;
