CREATE OR REFRESH STREAMING TABLE rw_dlt_tests.transactions_gold (
    transaction_date    DATE          NOT NULL,
    amount              DECIMAL(10,2),
    amount_with_tax     DECIMAL(10,2)
)
CLUSTER BY (transaction_date)
TBLPROPERTIES (
    'delta.appendOnly' = true
)
AS
SELECT
    transaction_date,
    amount,
    amount_with_tax
FROM STREAM(rw_dlt_tests.transactions_silver);
