CREATE OR REFRESH STREAMING TABLE my_table
  TBLPROPERTIES ('delta.enableTypeWidening' = 'true') AS
SELECT
  *
FROM
  STREAM(source_table);