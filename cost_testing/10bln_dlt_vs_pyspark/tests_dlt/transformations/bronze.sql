CREATE OR REFRESH STREAMING TABLE rw_dlt_tests.transactions_bronze (
)
CLUSTER BY (transaction_date)
TBLPROPERTIES (
    'delta.appendOnly' = true
)
AS
SELECT
    transaction_id,
    transaction_date,
    transaction_time,
    amount,
    tax,
    description
FROM STREAM(tests_source.transactions10m);
