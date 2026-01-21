CREATE OR REPLACE STREAMING TABLE main.default.dlt_transactions_silver  AS
SELECT
  *
FROM STREAM(main.default.pages);