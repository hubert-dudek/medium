# Power BI DirectQuery demo for Databricks

This bundle creates a small synthetic retail analytics model in Databricks so you can connect Power BI Desktop in **DirectQuery** mode, build a report, export it as `.pbit`, and then test the Databricks AI/BI / Genie Code import flow.

The bundle creates:

- `dim_date`
- `dim_product`
- `dim_customer`
- `fact_sales`
- DirectQuery-friendly views:
  - `vw_powerbi_sales_detail`
  - `vw_powerbi_daily_kpi`
  - `vw_powerbi_monthly_category`
  - `vw_powerbi_product_rank`
  - `vw_powerbi_kpi_tiles`
  - `vw_powerbi_recent_orders`

Default target location:

```text
main.powerbi_directquery_demo
```

## Deploy and run

From this folder:

```bash
databricks bundle validate --var="sql_warehouse_id=<YOUR_SQL_WAREHOUSE_ID>"
databricks bundle deploy --var="sql_warehouse_id=<YOUR_SQL_WAREHOUSE_ID>"
databricks bundle run build_powerbi_directquery_demo --var="sql_warehouse_id=<YOUR_SQL_WAREHOUSE_ID>"
```

Optional overrides:

```bash
databricks bundle deploy \
  --var="sql_warehouse_id=<YOUR_SQL_WAREHOUSE_ID>" \
  --var="catalog=<CATALOG>" \
  --var="schema=<SCHEMA>" \
  --var="fact_rows=1000000"

databricks bundle run build_powerbi_directquery_demo \
  --var="sql_warehouse_id=<YOUR_SQL_WAREHOUSE_ID>" \
  --var="catalog=<CATALOG>" \
  --var="schema=<SCHEMA>" \
  --var="fact_rows=1000000"
```

For the first test, keep `fact_rows=250000`. After the Power BI report works, increase it to 1M or more.

## Power BI Desktop connection

Use the same SQL warehouse that ran the bundle.

1. Open **Power BI Desktop**.
2. Select **Get data** → **Databricks** or **Azure Databricks**.
3. Enter the SQL warehouse **Server Hostname** and **HTTP Path**.
4. Choose **DirectQuery**.
5. Authenticate with OAuth or a personal access token.
6. In Navigator, choose either:
   - easiest: `vw_powerbi_sales_detail`, `vw_powerbi_daily_kpi`, `vw_powerbi_monthly_category`, `vw_powerbi_product_rank`, `vw_powerbi_kpi_tiles`, or
   - star schema: `fact_sales`, `dim_date`, `dim_product`, `dim_customer`.

For a fast report, prefer the aggregate views for visuals and keep the detail view only for drill-through/detail tables.

## Suggested report pages

### Page 1 — Sales overview

Use these visuals:

- Cards from `vw_powerbi_kpi_tiles`:
  - `total_net_revenue`
  - `completed_orders`
  - `active_customers`
  - `total_margin_pct`
  - `net_revenue_last_30_days`
- Line chart:
  - Axis: `vw_powerbi_daily_kpi.order_date`
  - Values: `net_revenue`
- Bar chart:
  - Axis: `vw_powerbi_daily_kpi.category`
  - Values: `net_revenue`
- Slicers:
  - `country`
  - `category`
  - `sales_channel`
  - `year_month`

### Page 2 — Product performance

Use these visuals:

- Table from `vw_powerbi_product_rank`:
  - `revenue_rank`
  - `product_name`
  - `category`
  - `brand`
  - `units_sold`
  - `net_revenue`
  - `margin_pct`
- Bar chart:
  - Axis: `product_name`
  - Values: `net_revenue`
  - Filter: `revenue_rank <= 20`

### Page 3 — Recent orders

Use `vw_powerbi_recent_orders` for a table visual:

- `order_ts`
- `customer_name`
- `country`
- `product_name`
- `sales_channel`
- `quantity`
- `net_revenue`
- `margin_amount`

## Optional DAX measures

If you import the wide view `vw_powerbi_sales_detail`, these measures are enough for a simple dashboard:

```DAX
Total Net Revenue = SUM('vw_powerbi_sales_detail'[net_revenue])

Orders = DISTINCTCOUNT('vw_powerbi_sales_detail'[sales_id])

Units Sold = SUM('vw_powerbi_sales_detail'[quantity])

Gross Margin % =
DIVIDE(
    SUM('vw_powerbi_sales_detail'[margin_amount]),
    SUM('vw_powerbi_sales_detail'[net_revenue])
)

Average Order Value = DIVIDE([Total Net Revenue], [Orders])

Return Rate =
DIVIDE(
    CALCULATE(
        [Orders],
        'vw_powerbi_sales_detail'[order_status] = "Returned"
    ),
    [Orders]
)
```

For DirectQuery performance, prefer measures over the aggregate views when possible, for example `vw_powerbi_daily_kpi` and `vw_powerbi_monthly_category`.

## Export `.pbit` for Databricks import test

After creating visuals in Power BI Desktop:

1. Save the `.pbix` locally for your own copy.
2. Select **File** → **Export** → **Power BI template**.
3. Save as `.pbit`.
4. Upload the `.pbit` into the Databricks Genie Code / AI-BI import flow, or place it in a Unity Catalog Volume and reference it from there.

This repository intentionally does not include a generated `.pbit` binary. Power BI Desktop should create the template after your real DirectQuery connection has the correct Databricks workspace, SQL warehouse, catalog, schema, and credentials.

## Useful validation SQL

```sql
SELECT * FROM main.powerbi_directquery_demo.vw_powerbi_kpi_tiles;

SELECT order_date, SUM(net_revenue) AS net_revenue
FROM main.powerbi_directquery_demo.vw_powerbi_daily_kpi
GROUP BY order_date
ORDER BY order_date DESC;

SELECT product_name, category, net_revenue, revenue_rank
FROM main.powerbi_directquery_demo.vw_powerbi_product_rank
WHERE revenue_rank <= 20
ORDER BY revenue_rank;
```
