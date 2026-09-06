-- Databricks SQL task file for a Power BI DirectQuery demo dataset.
-- Parameters are injected by the DAB SQL task using Databricks SQL named markers: :catalog, :schema, :fact_rows.

USE CATALOG IDENTIFIER(:catalog);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:schema)
COMMENT 'Synthetic retail analytics schema for testing Power BI DirectQuery and Databricks AI/BI dashboard import flows';
USE SCHEMA IDENTIFIER(:schema);

-- Re-runnable cleanup. Views are dropped first because they depend on the tables.
DROP VIEW IF EXISTS vw_powerbi_kpi_tiles;
DROP VIEW IF EXISTS vw_powerbi_sales_detail;
DROP VIEW IF EXISTS vw_powerbi_daily_kpi;
DROP VIEW IF EXISTS vw_powerbi_monthly_category;
DROP VIEW IF EXISTS vw_powerbi_product_rank;
DROP VIEW IF EXISTS vw_powerbi_recent_orders;
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date (
  date_id INT NOT NULL COMMENT 'Calendar date key in yyyyMMdd format',
  calendar_date DATE NOT NULL COMMENT 'Calendar date',
  day_name STRING NOT NULL COMMENT 'Day name',
  week_number INT NOT NULL COMMENT 'ISO-like week number',
  month_number INT NOT NULL COMMENT 'Month number from 1 to 12',
  month_name STRING NOT NULL COMMENT 'Short month name',
  quarter_number INT NOT NULL COMMENT 'Quarter number from 1 to 4',
  year_number INT NOT NULL COMMENT 'Calendar year',
  year_month STRING NOT NULL COMMENT 'Year-month label yyyy-MM',
  is_weekend BOOLEAN NOT NULL COMMENT 'True for Saturday and Sunday',
  CONSTRAINT dim_date_pk PRIMARY KEY(date_id) NOT ENFORCED RELY
)
USING DELTA
COMMENT 'Date dimension for retail sales Power BI DirectQuery demo';

INSERT INTO dim_date
WITH ds AS (
  SELECT explode(sequence(to_date('2025-01-01'), current_date(), interval 1 day)) AS calendar_date
)
SELECT
  CAST(date_format(calendar_date, 'yyyyMMdd') AS INT) AS date_id,
  calendar_date,
  date_format(calendar_date, 'E') AS day_name,
  weekofyear(calendar_date) AS week_number,
  month(calendar_date) AS month_number,
  date_format(calendar_date, 'MMM') AS month_name,
  quarter(calendar_date) AS quarter_number,
  year(calendar_date) AS year_number,
  date_format(calendar_date, 'yyyy-MM') AS year_month,
  CASE WHEN dayofweek(calendar_date) IN (1, 7) THEN true ELSE false END AS is_weekend
FROM ds;

CREATE TABLE dim_product (
  product_id INT NOT NULL COMMENT 'Product key',
  sku STRING NOT NULL COMMENT 'Synthetic SKU',
  product_name STRING NOT NULL COMMENT 'Product display name',
  category STRING NOT NULL COMMENT 'Business product category',
  subcategory STRING NOT NULL COMMENT 'Business product subcategory',
  brand STRING NOT NULL COMMENT 'Synthetic brand',
  list_price DECIMAL(10,2) NOT NULL COMMENT 'Base list price in EUR',
  standard_margin_pct DECIMAL(5,2) NOT NULL COMMENT 'Expected gross margin percent as a decimal',
  is_active BOOLEAN NOT NULL COMMENT 'Synthetic active/inactive flag',
  CONSTRAINT dim_product_pk PRIMARY KEY(product_id) NOT ENFORCED RELY
)
USING DELTA
COMMENT 'Product dimension for retail sales Power BI DirectQuery demo';

INSERT INTO dim_product
WITH p AS (
  SELECT CAST(id + 1 AS INT) AS product_id FROM range(80)
), mapped AS (
  SELECT
    product_id,
    element_at(array('Electronics','Home','Sports','Fashion','Beauty','Toys','Books','Garden'), pmod(product_id - 1, 8) + 1) AS category,
    element_at(array('Basic','Standard','Premium','Eco','Pro','Mini','Max','Limited'), pmod(product_id * 3, 8) + 1) AS subcategory,
    element_at(array('Northstar','Contoso','Fabrikam','BlueYard','Alpine','Novara'), pmod(product_id * 5, 6) + 1) AS brand
  FROM p
)
SELECT
  product_id,
  concat('SKU-', lpad(CAST(product_id AS STRING), 4, '0')) AS sku,
  concat(brand, ' ', subcategory, ' ', category, ' ', CAST(product_id AS STRING)) AS product_name,
  category,
  subcategory,
  brand,
  CAST(10 + pmod(hash(product_id * 31), 490) + pmod(product_id, 100) / 100.0 AS DECIMAL(10,2)) AS list_price,
  CAST((20 + pmod(hash(product_id * 17), 60)) / 100.0 AS DECIMAL(5,2)) AS standard_margin_pct,
  CASE WHEN pmod(product_id, 13) = 0 THEN false ELSE true END AS is_active
FROM mapped;

CREATE TABLE dim_customer (
  customer_id INT NOT NULL COMMENT 'Customer key',
  customer_name STRING NOT NULL COMMENT 'Synthetic customer display name',
  country STRING NOT NULL COMMENT 'Customer country',
  region STRING NOT NULL COMMENT 'Reporting region',
  customer_segment STRING NOT NULL COMMENT 'Customer segment',
  signup_date DATE NOT NULL COMMENT 'Synthetic customer signup date',
  loyalty_tier STRING NOT NULL COMMENT 'Synthetic loyalty tier',
  CONSTRAINT dim_customer_pk PRIMARY KEY(customer_id) NOT ENFORCED RELY
)
USING DELTA
COMMENT 'Customer dimension for retail sales Power BI DirectQuery demo';

INSERT INTO dim_customer
WITH c AS (
  SELECT CAST(id + 1 AS INT) AS customer_id FROM range(5000)
), mapped AS (
  SELECT
    customer_id,
    element_at(array('Poland','Czechia','Germany','France','Spain','Italy','Netherlands','Sweden'), pmod(hash(customer_id * 11), 8) + 1) AS country,
    element_at(array('Consumer','Small Business','Enterprise','Public Sector'), pmod(hash(customer_id * 13), 4) + 1) AS customer_segment,
    element_at(array('Bronze','Silver','Gold','Platinum'), pmod(hash(customer_id * 19), 4) + 1) AS loyalty_tier
  FROM c
)
SELECT
  customer_id,
  concat('Customer ', CAST(customer_id AS STRING)) AS customer_name,
  country,
  CASE
    WHEN country IN ('Poland','Czechia','Germany','Netherlands') THEN 'Central Europe'
    WHEN country IN ('France','Spain','Italy') THEN 'Southern Europe'
    ELSE 'Northern Europe'
  END AS region,
  customer_segment,
  date_add(to_date('2023-01-01'), pmod(hash(customer_id * 23), datediff(current_date(), to_date('2023-01-01')) + 1)) AS signup_date,
  loyalty_tier
FROM mapped;

CREATE TABLE fact_sales (
  sales_id BIGINT NOT NULL COMMENT 'Synthetic sales transaction key',
  date_id INT NOT NULL COMMENT 'Order date key in yyyyMMdd format',
  order_date DATE NOT NULL COMMENT 'Order date',
  order_ts TIMESTAMP NOT NULL COMMENT 'Synthetic order timestamp',
  customer_id INT NOT NULL COMMENT 'Customer key',
  product_id INT NOT NULL COMMENT 'Product key',
  order_status STRING NOT NULL COMMENT 'Completed, Returned, or Cancelled',
  sales_channel STRING NOT NULL COMMENT 'Sales channel',
  quantity INT NOT NULL COMMENT 'Units sold',
  unit_price DECIMAL(10,2) NOT NULL COMMENT 'Unit selling price in EUR',
  gross_revenue DECIMAL(18,2) NOT NULL COMMENT 'Quantity multiplied by unit price',
  discount_pct DECIMAL(5,2) NOT NULL COMMENT 'Discount percent as a decimal',
  discount_amount DECIMAL(18,2) NOT NULL COMMENT 'Gross revenue multiplied by discount percent',
  net_revenue DECIMAL(18,2) NOT NULL COMMENT 'Gross revenue after discount',
  cost_amount DECIMAL(18,2) NOT NULL COMMENT 'Synthetic cost amount',
  margin_amount DECIMAL(18,2) NOT NULL COMMENT 'Net revenue minus cost amount',
  currency STRING NOT NULL COMMENT 'Currency code',
  CONSTRAINT fact_sales_pk PRIMARY KEY(sales_id) NOT ENFORCED RELY,
  CONSTRAINT fact_sales_date_fk FOREIGN KEY(date_id) REFERENCES dim_date(date_id) NOT ENFORCED RELY,
  CONSTRAINT fact_sales_product_fk FOREIGN KEY(product_id) REFERENCES dim_product(product_id) NOT ENFORCED RELY,
  CONSTRAINT fact_sales_customer_fk FOREIGN KEY(customer_id) REFERENCES dim_customer(customer_id) NOT ENFORCED RELY
)
USING DELTA
CLUSTER BY (order_date, product_id, customer_id)
COMMENT 'Synthetic retail sales fact table for Power BI DirectQuery demos';

INSERT INTO fact_sales
WITH settings AS (
  SELECT datediff(current_date(), to_date('2025-01-01')) AS day_count
), src AS (
  SELECT CAST(id + 1 AS BIGINT) AS sales_id FROM range(CAST(:fact_rows AS BIGINT))
), base AS (
  SELECT
    src.sales_id,
    date_add(to_date('2025-01-01'), pmod(hash(src.sales_id * 7), settings.day_count + 1)) AS order_date,
    1 + pmod(hash(src.sales_id * 17), 5000) AS customer_id,
    1 + pmod(hash(src.sales_id * 23), 80) AS product_id,
    1 + pmod(hash(src.sales_id * 3), 5) AS quantity,
    pmod(hash(src.sales_id * 37), 24) AS hour_of_day,
    pmod(hash(src.sales_id * 41), 60) AS minute_of_hour,
    CASE
      WHEN pmod(hash(src.sales_id * 43), 100) < 4 THEN 'Cancelled'
      WHEN pmod(hash(src.sales_id * 47), 100) < 8 THEN 'Returned'
      ELSE 'Completed'
    END AS order_status,
    element_at(array('Web','Mobile','Store','Marketplace'), pmod(hash(src.sales_id * 53), 4) + 1) AS sales_channel,
    CASE
      WHEN pmod(hash(src.sales_id * 59), 100) < 10 THEN CAST(0.20 AS DECIMAL(5,2))
      WHEN pmod(hash(src.sales_id * 59), 100) < 30 THEN CAST(0.10 AS DECIMAL(5,2))
      ELSE CAST(0.00 AS DECIMAL(5,2))
    END AS discount_pct
  FROM src CROSS JOIN settings
), priced AS (
  SELECT
    b.*,
    p.standard_margin_pct,
    CAST(p.list_price * (0.85 + pmod(hash(b.sales_id * 61), 31) / 100.0) AS DECIMAL(10,2)) AS unit_price
  FROM base b
  INNER JOIN dim_product p ON b.product_id = p.product_id
), amounts AS (
  SELECT
    sales_id,
    order_date,
    to_timestamp(concat(CAST(order_date AS STRING), ' ', lpad(CAST(hour_of_day AS STRING), 2, '0'), ':', lpad(CAST(minute_of_hour AS STRING), 2, '0'), ':00')) AS order_ts,
    customer_id,
    product_id,
    order_status,
    sales_channel,
    quantity,
    unit_price,
    discount_pct,
    standard_margin_pct,
    CAST(quantity * unit_price AS DECIMAL(18,2)) AS gross_revenue,
    CAST(quantity * unit_price * discount_pct AS DECIMAL(18,2)) AS discount_amount,
    CAST((quantity * unit_price) - (quantity * unit_price * discount_pct) AS DECIMAL(18,2)) AS net_revenue
  FROM priced
)
SELECT
  sales_id,
  CAST(date_format(order_date, 'yyyyMMdd') AS INT) AS date_id,
  order_date,
  order_ts,
  customer_id,
  product_id,
  order_status,
  sales_channel,
  quantity,
  unit_price,
  gross_revenue,
  discount_pct,
  discount_amount,
  net_revenue,
  CAST(net_revenue * (1 - standard_margin_pct) AS DECIMAL(18,2)) AS cost_amount,
  CAST(net_revenue - (net_revenue * (1 - standard_margin_pct)) AS DECIMAL(18,2)) AS margin_amount,
  'EUR' AS currency
FROM amounts;

CREATE OR REPLACE VIEW vw_powerbi_sales_detail
COMMENT 'Wide denormalized view: easiest DirectQuery table for a first Power BI report'
AS
SELECT
  f.sales_id,
  f.order_date,
  f.order_ts,
  d.day_name,
  d.week_number,
  d.month_number,
  d.month_name,
  d.quarter_number,
  d.year_number,
  d.year_month,
  d.is_weekend,
  c.customer_id,
  c.customer_name,
  c.country,
  c.region,
  c.customer_segment,
  c.loyalty_tier,
  p.product_id,
  p.sku,
  p.product_name,
  p.category,
  p.subcategory,
  p.brand,
  f.order_status,
  f.sales_channel,
  f.quantity,
  f.unit_price,
  f.gross_revenue,
  f.discount_pct,
  f.discount_amount,
  f.net_revenue,
  f.cost_amount,
  f.margin_amount,
  f.currency
FROM fact_sales f
INNER JOIN dim_date d ON f.date_id = d.date_id
INNER JOIN dim_customer c ON f.customer_id = c.customer_id
INNER JOIN dim_product p ON f.product_id = p.product_id;

CREATE OR REPLACE VIEW vw_powerbi_daily_kpi
COMMENT 'Daily KPI aggregate for fast Power BI DirectQuery visuals'
AS
SELECT
  order_date,
  year_number,
  year_month,
  category,
  country,
  region,
  customer_segment,
  sales_channel,
  COUNT(*) AS completed_orders,
  COUNT(DISTINCT customer_id) AS active_customers,
  SUM(quantity) AS units_sold,
  CAST(SUM(gross_revenue) AS DECIMAL(18,2)) AS gross_revenue,
  CAST(SUM(discount_amount) AS DECIMAL(18,2)) AS discount_amount,
  CAST(SUM(net_revenue) AS DECIMAL(18,2)) AS net_revenue,
  CAST(SUM(margin_amount) AS DECIMAL(18,2)) AS margin_amount,
  CAST(CASE WHEN SUM(net_revenue) = 0 THEN NULL ELSE SUM(margin_amount) / SUM(net_revenue) END AS DECIMAL(8,4)) AS margin_pct
FROM vw_powerbi_sales_detail
WHERE order_status = 'Completed'
GROUP BY order_date, year_number, year_month, category, country, region, customer_segment, sales_channel;

CREATE OR REPLACE VIEW vw_powerbi_monthly_category
COMMENT 'Monthly product-category aggregate for trend visuals'
AS
SELECT
  date_trunc('month', order_date) AS month_start,
  year_month,
  category,
  subcategory,
  brand,
  COUNT(*) AS completed_orders,
  SUM(quantity) AS units_sold,
  CAST(SUM(net_revenue) AS DECIMAL(18,2)) AS net_revenue,
  CAST(SUM(margin_amount) AS DECIMAL(18,2)) AS margin_amount,
  CAST(CASE WHEN SUM(net_revenue) = 0 THEN NULL ELSE SUM(margin_amount) / SUM(net_revenue) END AS DECIMAL(8,4)) AS margin_pct
FROM vw_powerbi_sales_detail
WHERE order_status = 'Completed'
GROUP BY date_trunc('month', order_date), year_month, category, subcategory, brand;

CREATE OR REPLACE VIEW vw_powerbi_product_rank
COMMENT 'Product ranking view for Power BI table and bar-chart visuals'
AS
WITH product_sales AS (
  SELECT
    product_id,
    sku,
    product_name,
    category,
    subcategory,
    brand,
    COUNT(*) AS completed_orders,
    SUM(quantity) AS units_sold,
    CAST(SUM(net_revenue) AS DECIMAL(18,2)) AS net_revenue,
    CAST(SUM(margin_amount) AS DECIMAL(18,2)) AS margin_amount,
    CAST(CASE WHEN SUM(net_revenue) = 0 THEN NULL ELSE SUM(margin_amount) / SUM(net_revenue) END AS DECIMAL(8,4)) AS margin_pct
  FROM vw_powerbi_sales_detail
  WHERE order_status = 'Completed'
  GROUP BY product_id, sku, product_name, category, subcategory, brand
)
SELECT
  *,
  DENSE_RANK() OVER (ORDER BY net_revenue DESC) AS revenue_rank
FROM product_sales;

CREATE OR REPLACE VIEW vw_powerbi_kpi_tiles
COMMENT 'Single-row KPI view for Power BI card visuals'
AS
SELECT
  current_timestamp() AS generated_at,
  COUNT(*) AS total_order_rows,
  SUM(CASE WHEN order_status = 'Completed' THEN 1 ELSE 0 END) AS completed_orders,
  SUM(CASE WHEN order_status = 'Returned' THEN 1 ELSE 0 END) AS returned_orders,
  SUM(CASE WHEN order_status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
  COUNT(DISTINCT CASE WHEN order_status = 'Completed' THEN customer_id ELSE NULL END) AS active_customers,
  CAST(SUM(CASE WHEN order_status = 'Completed' THEN net_revenue ELSE 0 END) AS DECIMAL(18,2)) AS total_net_revenue,
  CAST(SUM(CASE WHEN order_status = 'Completed' THEN margin_amount ELSE 0 END) AS DECIMAL(18,2)) AS total_margin_amount,
  CAST(
    CASE
      WHEN SUM(CASE WHEN order_status = 'Completed' THEN net_revenue ELSE 0 END) = 0 THEN NULL
      ELSE SUM(CASE WHEN order_status = 'Completed' THEN margin_amount ELSE 0 END) /
           SUM(CASE WHEN order_status = 'Completed' THEN net_revenue ELSE 0 END)
    END AS DECIMAL(8,4)
  ) AS total_margin_pct,
  CAST(SUM(CASE WHEN order_status = 'Completed' AND order_date >= date_add(current_date(), -30) THEN net_revenue ELSE 0 END) AS DECIMAL(18,2)) AS net_revenue_last_30_days
FROM vw_powerbi_sales_detail;

CREATE OR REPLACE VIEW vw_powerbi_recent_orders
COMMENT 'Recent completed orders for detail table visuals and data freshness checks'
AS
SELECT *
FROM vw_powerbi_sales_detail
WHERE order_status = 'Completed'
  AND order_date >= date_add(current_date(), -30);

ANALYZE TABLE dim_date COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE dim_product COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE dim_customer COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE fact_sales COMPUTE STATISTICS FOR ALL COLUMNS;
OPTIMIZE fact_sales;

SELECT 'dim_date' AS object_name, COUNT(*) AS row_count FROM dim_date
UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'fact_sales', COUNT(*) FROM fact_sales
UNION ALL SELECT 'vw_powerbi_daily_kpi', COUNT(*) FROM vw_powerbi_daily_kpi
UNION ALL SELECT 'vw_powerbi_product_rank', COUNT(*) FROM vw_powerbi_product_rank;
