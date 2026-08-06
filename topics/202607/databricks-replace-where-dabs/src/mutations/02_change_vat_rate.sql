-- Databricks notebook source
-- Correct only the VAT rule effective from 2026-01-01. The demo job passes the
-- same date as vat_recompute_from, leaving December 2025 target rows untouched.

USE CATALOG IDENTIFIER(:source_catalog);
USE SCHEMA IDENTIFIER(:source_schema);

UPDATE vat_rate_dim
SET vat_rate = 0.2400,
    rule_version = 'CZ-STD-2026-corrected',
    updated_at = CURRENT_TIMESTAMP()
WHERE country_code = 'CZ'
  AND product_type = 'STANDARD'
  AND valid_from = DATE '2026-01-01';

SELECT *
FROM vat_rate_dim
WHERE country_code = 'CZ' AND product_type = 'STANDARD'
ORDER BY valid_from;
