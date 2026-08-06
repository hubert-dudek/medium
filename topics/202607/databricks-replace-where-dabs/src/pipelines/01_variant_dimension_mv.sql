-- Use case 1
-- A large materialized result joins facts with a current VARIANT dimension.
-- recompute_days controls how far back a changed specification is propagated.
-- This file intentionally uses the Materialized View REPLACE preview syntax.

CREATE OR REFRESH MATERIALIZED VIEW sales_by_product_spec
FLOW REPLACE WHERE
  order_date >= DATE_SUB(CURRENT_DATE(), CAST(:recompute_days AS INT))
BY NAME
WITH daily_product_sales AS (
  SELECT
    order_date,
    product_id,
    COUNT(*) AS order_count,
    SUM(quantity) AS units_sold,
    CAST(SUM(quantity * unit_price) AS DECIMAL(18, 2)) AS net_revenue,
    MAX(source_updated_at) AS last_fact_update
  FROM IDENTIFIER(:source_catalog || '.' || :source_schema || '.orders_fact')
  GROUP BY order_date, product_id
)
SELECT
  f.order_date,
  f.product_id,
  d.product_name,
  d.product_spec,
  TRY_VARIANT_GET(d.product_spec, '$.family', 'STRING') AS product_family,
  TRY_VARIANT_GET(d.product_spec, '$.tier', 'STRING') AS product_tier,
  TRY_VARIANT_GET(d.product_spec, '$.hardware.ram_gb', 'INT') AS ram_gb,
  TRY_VARIANT_GET(d.product_spec, '$.energy.class', 'STRING') AS energy_class,
  d.spec_version,
  SHA2(CAST(d.product_spec AS STRING), 256) AS product_spec_hash,
  f.order_count,
  f.units_sold,
  f.net_revenue,
  f.last_fact_update,
  d.updated_at AS last_dimension_update
FROM daily_product_sales AS f
JOIN IDENTIFIER(:source_catalog || '.' || :source_schema || '.product_spec_dim') AS d
  ON f.product_id = d.product_id;
