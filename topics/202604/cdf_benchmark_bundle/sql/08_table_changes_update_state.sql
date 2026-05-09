SET QUERY_TAGS['benchmark_test'] = 'table_changes_sql';

INSERT INTO IDENTIFIER(:state_table)
SELECT
  :pipeline_name AS pipeline_name,
  CAST(:last_commit_version AS BIGINT) AS last_commit_version,
  current_timestamp() AS updated_at;
