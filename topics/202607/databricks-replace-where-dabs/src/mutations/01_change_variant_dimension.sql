-- Databricks notebook source
-- Product 1 receives a new nested JSON specification. After the normal pipeline
-- run, only the most recent recompute_days retain the new spec/version.

USE CATALOG IDENTIFIER(:source_catalog);
USE SCHEMA IDENTIFIER(:source_schema);

UPDATE product_spec_dim
SET product_spec = PARSE_JSON('{"family":"laptop","tier":"premium","hardware":{"ram_gb":24,"storage_gb":1024},"energy":{"class":"A+"},"connectivity":["wifi7","usb-c","thunderbolt"],"support":{"years":4}}'),
    spec_version = 2,
    updated_at = CURRENT_TIMESTAMP()
WHERE product_id = 1;

SELECT product_id, product_name, spec_version, product_spec, updated_at
FROM product_spec_dim
WHERE product_id = 1;
