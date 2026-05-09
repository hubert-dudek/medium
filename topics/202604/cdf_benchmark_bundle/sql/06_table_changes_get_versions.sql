SET QUERY_TAGS['benchmark_test'] = 'table_changes_sql';

CREATE TABLE IF NOT EXISTS IDENTIFIER(:target_table) (
  booking_id BIGINT,
  customer_id BIGINT,
  property_id BIGINT,
  booking_date DATE,
  checkin_date DATE,
  checkout_date DATE,
  amount DECIMAL(12,2),
  status STRING,
  updated_at TIMESTAMP
) 
CLUSTER BY (booking_date);

CREATE TABLE IF NOT EXISTS IDENTIFIER(:state_table) (
  pipeline_name STRING,
  last_commit_version BIGINT,
  updated_at TIMESTAMP
)
USING DELTA;

DECLARE OR REPLACE VARIABLE start_v BIGINT DEFAULT 0;

SET VAR start_v = (
  SELECT COALESCE(
    (
      SELECT last_commit_version + 1
      FROM IDENTIFIER(:state_table)
      WHERE pipeline_name = :pipeline_name
      ORDER BY updated_at DESC
      LIMIT 1
    ),
    0
  )
);

SELECT
  start_v AS start_version,
  CASE
    WHEN version < start_v THEN start_v
    ELSE version
  END AS end_version
FROM (
  DESCRIBE HISTORY IDENTIFIER('workspace.cdf_benchmark_source.bookings') LIMIT 1
);