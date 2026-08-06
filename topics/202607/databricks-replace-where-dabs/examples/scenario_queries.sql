-- Convenience queries for Databricks SQL.
-- Replace the catalog/schema placeholders with the deployed values if not using
-- the bundle defaults: main.replace_where_demo_out_dev and main.replace_where_demo_src_dev.

-- 1. Recent rows adopt the new VARIANT dimension; older rows stay frozen.
SELECT order_date, product_id, spec_version, product_spec, net_revenue
FROM main.replace_where_demo_out_dev.sales_by_product_spec
WHERE product_id = 1
ORDER BY order_date DESC;

-- 2. Compare VAT before and after the legal boundary.
SELECT invoice_date, invoice_id, vat_rate, rule_version, vat_amount
FROM main.replace_where_demo_out_dev.invoice_vat_calculation
ORDER BY invoice_date;

-- 3. Latest weather snapshot for today forward.
SELECT *
FROM main.replace_where_demo_out_dev.weather_forecast_current
ORDER BY location_id, forecast_date;

-- 4. Monitor status and pipeline update IDs.
SELECT *
FROM main.replace_where_demo_src_dev.backfill_requests
ORDER BY requested_at DESC;

-- 5. Closed versus open accounting dates.
SELECT accounting_date, invoice_id, cost_center, allocation_version, commission_eur
FROM main.replace_where_demo_out_dev.invoice_reporting_open_period
ORDER BY accounting_date;

-- 6. Corrected file replaces the complete day, including removals.
SELECT provider, business_date, transaction_id, fee_amount, source_file_version
FROM main.replace_where_demo_out_dev.settlement_canonical
ORDER BY business_date, transaction_id;
