-- Use case 2
-- Recompute invoices only from the legal effective date of a changed VAT rule.
-- Older tax calculations stay frozen even though the dimension is queried again.
-- This file intentionally uses the Materialized View REPLACE preview syntax.

CREATE OR REFRESH MATERIALIZED VIEW invoice_vat_calculation
FLOW REPLACE WHERE
  invoice_date >= CAST(:vat_recompute_from AS DATE)
BY NAME
SELECT
  i.invoice_date,
  i.invoice_line_id,
  i.invoice_id,
  i.country_code,
  i.product_type,
  i.net_amount,
  v.vat_rate,
  CAST(ROUND(i.net_amount * v.vat_rate, 2) AS DECIMAL(18, 2)) AS vat_amount,
  CAST(ROUND(i.net_amount * (1 + v.vat_rate), 2) AS DECIMAL(18, 2)) AS gross_amount,
  v.valid_from AS vat_valid_from,
  v.valid_to AS vat_valid_to,
  v.rule_version,
  GREATEST(i.source_updated_at, v.updated_at) AS calculation_source_updated_at
FROM IDENTIFIER(:source_catalog || '.' || :source_schema || '.invoice_lines') AS i
JOIN IDENTIFIER(:source_catalog || '.' || :source_schema || '.vat_rate_dim') AS v
  ON i.country_code = v.country_code
 AND i.product_type = v.product_type
 AND i.invoice_date >= v.valid_from
 AND (v.valid_to IS NULL OR i.invoice_date < v.valid_to);
