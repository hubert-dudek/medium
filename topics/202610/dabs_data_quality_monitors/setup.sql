CREATE SCHEMA IF NOT EXISTS news_dabs_20261104.news_dabs_20261104_dq;

CREATE TABLE news_dabs_20261104.news_dabs_20261104_dq.sales
USING DELTA AS
SELECT * FROM VALUES
  (1, 10.0),
  (2, 20.0),
  (3, 30.0),
  (4, NULL)
AS sales(order_id, amount);
