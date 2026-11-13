CREATE SCHEMA IF NOT EXISTS sbx_test_data.news_ai_functions_20261113;

CREATE TABLE sbx_test_data.news_ai_functions_20261113.tickets
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
AS SELECT * FROM VALUES
  (1, 'My invoice has an unexpected charge.'),
  (2, 'The application crashes when I sign in.')
AS tickets(id, body);
