-- 1) Add per-row duplicate count (cnt) using a window
-- Use PRIVATE so it’s an intermediate dataset (not published to the catalog)
CREATE OR REFRESH PRIVATE MATERIALIZED VIEW country_with_cnt AS
SELECT
  c.*,
  RANK(*) OVER (PARTITION BY country_id ORDER BY timestamp) AS cnt
FROM
  countries c;

-- 2) Clean dataset with a data-quality rule:
-- Drop anything where cnt != 1 (so all rows for duplicated keys are rejected)
CREATE OR REFRESH MATERIALIZED VIEW country_silver (
    CONSTRAINT unique_country_id EXPECT(cnt = 1) ON VIOLATION DROP ROW
  ) AS
SELECT
  country_id,
  country_name,
  cnt
FROM
  country_with_cnt;

-- 3) Quarantine dataset = the reverse condition (cnt > 1)
CREATE OR REFRESH MATERIALIZED VIEW country_duplicated (
    CONSTRAINT duplicated_country_id EXPECT(cnt > 1) ON VIOLATION DROP ROW
  ) AS
SELECT
  country_id,
  country_name,
  cnt
FROM
  country_with_cnt;