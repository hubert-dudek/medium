SET QUERY_TAGS['benchmark_test'] = 'table_changes_sql';

DECLARE OR REPLACE VARIABLE latest_per_key_sql STRING;

SET VAR latest_per_key_sql = "
CREATE OR REPLACE TEMP VIEW staged AS
WITH changes AS (
  SELECT *
  FROM table_changes('" || :source_table || "', " || :start_version || ", " || :end_version || ")
  WHERE _change_type <> 'update_preimage'
)
SELECT *
FROM (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY booking_id, booking_date
      ORDER BY updated_at DESC, _commit_version DESC, _commit_timestamp DESC
    ) AS rn
  FROM changes
)
WHERE rn = 1;
";

EXECUTE IMMEDIATE latest_per_key_sql;

MERGE INTO IDENTIFIER(:target_table) AS t
USING staged AS s
ON t.booking_date = s.booking_date AND t.booking_id = s.booking_id
WHEN MATCHED AND s._change_type = 'delete' AND s.updated_at >= t.updated_at THEN DELETE
WHEN MATCHED AND s._change_type IN ('insert', 'update_postimage') AND s.updated_at >= t.updated_at THEN UPDATE SET
  t.customer_id   = s.customer_id,
  t.property_id   = s.property_id,
  t.checkin_date  = s.checkin_date,
  t.checkout_date = s.checkout_date,
  t.amount        = s.amount,
  t.status        = s.status,
  t.updated_at    = s.updated_at
WHEN NOT MATCHED AND s._change_type IN ('insert', 'update_postimage') THEN INSERT (
  booking_id,
  customer_id,
  property_id,
  booking_date,
  checkin_date,
  checkout_date,
  amount,
  status,
  updated_at
) VALUES (
  s.booking_id,
  s.customer_id,
  s.property_id,
  s.booking_date,
  s.checkin_date,
  s.checkout_date,
  s.amount,
  s.status,
  s.updated_at
);
