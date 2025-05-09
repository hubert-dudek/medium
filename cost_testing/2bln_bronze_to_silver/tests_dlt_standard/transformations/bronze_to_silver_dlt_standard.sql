CREATE OR REFRESH STREAMING TABLE transactions2 (
    transaction_id      BIGINT        NOT NULL,
    transaction_date    DATE          NOT NULL,
    transaction_time    TIMESTAMP     NOT NULL,
    amount              DECIMAL(10,2),
    amount_with_tax     DECIMAL(10,2),
    description         STRING        NOT NULL,
    CONSTRAINT pk_id PRIMARY KEY (transaction_id)
)
CLUSTER BY (transaction_date)
TBLPROPERTIES (
    'delta.appendOnly' = true,
    'delta.enableChangeDataFeed' = true,
    'delta.enableRowTracking' = true
)
AS
SELECT
    transaction_id,
    transaction_date,
    transaction_time,
    amount,
    (amount + (amount * tax))::decimal(10,2) AS amount_with_tax,
    description
FROM STREAM(tests.bronze_source.transactions);
