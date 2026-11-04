CREATE SCHEMA IF NOT EXISTS news_dabs_20261104.news_dabs_20261104_fs;

CREATE TABLE news_dabs_20261104.news_dabs_20261104_fs.transactions
USING DELTA
AS SELECT
  customer_id,
  amount,
  CAST(date_sub(current_date(), days_ago) AS TIMESTAMP) AS event_ts
FROM VALUES
  (1, 100, 1),
  (1, 50, 2),
  (2, 75, 1)
AS source(customer_id, amount, days_ago);
