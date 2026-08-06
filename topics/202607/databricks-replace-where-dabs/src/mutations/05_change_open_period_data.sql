-- Databricks notebook source
-- Change one CLOSED June allocation and one OPEN July allocation. With
-- open_period_start=2026-07-01, only the July target row is recalculated.

USE CATALOG IDENTIFIER(:source_catalog);
USE SCHEMA IDENTIFIER(:source_schema);

UPDATE invoice_event
SET allocation_spec = PARSE_JSON('{"cost_center":"ADVISORY-CORRECTED","commission_pct":0.09,"sales_region":"CENTRAL","allocation_version":2}'),
    source_updated_at = CURRENT_TIMESTAMP()
WHERE legal_entity = 'PL01' AND invoice_id = 'ACC-100';

UPDATE invoice_event
SET allocation_spec = PARSE_JSON('{"cost_center":"TRAINING-CORRECTED","commission_pct":0.10,"sales_region":"SOUTH","allocation_version":2}'),
    source_updated_at = CURRENT_TIMESTAMP()
WHERE legal_entity = 'PL01' AND invoice_id = 'ACC-101';

SELECT legal_entity, accounting_date, invoice_id, allocation_spec, source_updated_at
FROM invoice_event
WHERE invoice_id IN ('ACC-100', 'ACC-101')
ORDER BY accounting_date;
