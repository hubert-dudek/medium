-- Databricks notebook source
-- Accept a corrected version of the PAYFAST file for current_date()-2.
-- T100 remains with a corrected fee; T200 is deliberately absent from version 2
-- and therefore disappears from the replaced target slice.

USE CATALOG IDENTIFIER(:source_catalog);
USE SCHEMA IDENTIFIER(:source_schema);

DELETE FROM settlement_manifest
WHERE provider = 'PAYFAST'
  AND business_date = DATE_SUB(CURRENT_DATE(), 2)
  AND file_version = 2;

DELETE FROM settlement_rows
WHERE provider = 'PAYFAST'
  AND business_date = DATE_SUB(CURRENT_DATE(), 2)
  AND file_version = 2;

INSERT INTO settlement_manifest VALUES
  ('PAYFAST', DATE_SUB(CURRENT_DATE(), 2), 2, CURRENT_TIMESTAMP(), 'ACCEPTED', 'payfast-d2-v2-corrected');

INSERT INTO settlement_rows VALUES
  ('PAYFAST', DATE_SUB(CURRENT_DATE(), 2), 2, 'T100', 100.00, 2.10,
   PARSE_JSON('{"status":"captured","card_country":"PL","correction_reason":"fee_recalculation"}')),
  ('PAYFAST', DATE_SUB(CURRENT_DATE(), 2), 2, 'T250',  75.00, 1.50,
   PARSE_JSON('{"status":"captured","card_country":"SK","correction_reason":"late_addition"}'));

SELECT m.*, r.transaction_id, r.gross_amount, r.fee_amount, r.attributes
FROM settlement_manifest AS m
LEFT JOIN settlement_rows AS r
  ON m.provider = r.provider
 AND m.business_date = r.business_date
 AND m.file_version = r.file_version
WHERE m.provider = 'PAYFAST'
  AND m.business_date = DATE_SUB(CURRENT_DATE(), 2)
ORDER BY m.file_version, r.transaction_id;
