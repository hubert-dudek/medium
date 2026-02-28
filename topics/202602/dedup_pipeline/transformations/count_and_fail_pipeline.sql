CREATE OR REFRESH MATERIALIZED VIEW country_pk_counts AS
SELECT
  country_id,
  COUNT(*) AS cnt
FROM
  countries
GROUP BY
  country_id;

-- Expectation: fail if any cnt != 1
CREATE OR REFRESH MATERIALIZED VIEW country_pk_assert (
    CONSTRAINT pk_is_unique EXPECT(cnt = 1) ON VIOLATION FAIL UPDATE
  ) AS
SELECT
  *
FROM
  country_pk_counts;