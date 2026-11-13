CREATE SCHEMA IF NOT EXISTS sbx_test_data.news_feature_materialization_20261113;

CREATE TABLE sbx_test_data.news_feature_materialization_20261113.transactions
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
AS SELECT
  customer_id,
  amount,
  CAST(date_sub(current_date(), days_ago) AS TIMESTAMP) AS event_ts
FROM VALUES
  (1, 100, 1),
  (1, 50, 2),
  (2, 75, 1)
AS source(customer_id, amount, days_ago);
