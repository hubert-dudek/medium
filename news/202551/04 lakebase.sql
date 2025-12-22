-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Project permissions
-- MAGIC It for now only protecting others to mess with our project.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![](images/2551_4.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### New dashboard

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![](images/2551_4b.png)

-- COMMAND ----------

-- test queries
SET statement_timeout = '10min';
-- ==============
-- Scale controls
-- ==============
DROP TABLE IF EXISTS _params;
CREATE TEMP TABLE _params(work_n bigint, group_mod int, text_mod int);
INSERT INTO _params VALUES (10000000, 100000, 2000); 
-- =========================
-- 1) Data generation (TEMP)
-- =========================
DROP TABLE IF EXISTS t_events;
CREATE TEMP TABLE t_events (
  id        bigint PRIMARY KEY,
  k         int,
  ts        timestamptz,
  v1        int,
  v2        numeric,
  payload   jsonb,
  note      text
) ON COMMIT DROP;

-- Insert synthetic rows
INSERT INTO t_events (id, k, ts, v1, v2, payload, note)
SELECT
  i AS id,
  (i % p.group_mod) AS k,
  now() - ((i % 86400) || ' seconds')::interval AS ts,
  (random() * 1000000)::int AS v1,
  round((random() * 10000)::numeric, 2) AS v2,
  jsonb_build_object(
    'user',  (i % 50000),
    'type',  CASE WHEN i % 10 = 0 THEN 'click'
                  WHEN i % 10 = 1 THEN 'view'
                  WHEN i % 10 = 2 THEN 'purchase'
                  ELSE 'other' END,
    'score', round((random() * 100)::numeric, 2),
    'tags',  to_jsonb(ARRAY[
              md5(i::text),
              md5((i+1)::text),
              md5((i+2)::text)
            ])
  ) AS payload,
  repeat(chr((65 + (i % 26))::int), ((i % p.text_mod) + 10)::int) AS note
FROM generate_series(1, (SELECT work_n FROM _params)) AS i
CROSS JOIN _params p;

ANALYZE t_events;

-- Helpful index for join/sort scenarios (TEMP index)
CREATE INDEX IF NOT EXISTS ix_t_events_k_ts ON t_events (k, ts DESC);

-- ===========================
-- 2) CPU + aggregation baseline
-- ===========================
SELECT
  count(*)                         AS rows,
  min(ts)                          AS min_ts,
  max(ts)                          AS max_ts,
  avg(v1)                          AS avg_v1,
  sum(v2)                          AS sum_v2,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY v1) AS p95_v1
FROM t_events;

-- ===========================
-- 3) Heavy sort (CPU + memory)
-- ===========================
-- Sorting by a computed expression forces work
SELECT id, k, v1
FROM t_events
ORDER BY md5((id::text || '-' || k::text)) DESC
LIMIT 200;

-- ===========================
-- 4) Window functions (CPU)
-- ===========================
-- Per key, rank by time; also compute rolling metrics
WITH ranked AS (
  SELECT
    id, k, ts, v1, v2,
    row_number() OVER (PARTITION BY k ORDER BY ts DESC) AS rn,
    avg(v1)      OVER (PARTITION BY k)                 AS avg_v1_by_k,
    sum(v2)      OVER (PARTITION BY k ORDER BY ts
                       ROWS BETWEEN 50 PRECEDING AND CURRENT ROW) AS rolling_sum_v2
  FROM t_events
)
SELECT *
FROM ranked
WHERE rn <= 5
ORDER BY k, rn;

-- ===========================
-- 5) Join workload (TEMP dimension)
-- ===========================
DROP TABLE IF EXISTS t_dim;
CREATE TEMP TABLE t_dim (
  k int PRIMARY KEY,
  label text,
  weight numeric
) ON COMMIT DROP;

INSERT INTO t_dim(k, label, weight)
SELECT
  k,
  'grp_' || k::text,
  round((random() * 10)::numeric, 3)
FROM generate_series(0, (SELECT group_mod-1 FROM _params)) k;

ANALYZE t_dim;

-- Join + group aggregate
SELECT
  d.label,
  count(*)                     AS cnt,
  round(avg(e.v2)::numeric, 2) AS avg_v2,
  max(e.ts)                    AS last_seen
FROM t_events e
JOIN t_dim d USING (k)
GROUP BY d.label
ORDER BY cnt DESC
LIMIT 50;

-- ===========================
-- 6) JSON + text processing
-- ===========================
-- Extract fields from JSONB and filter
SELECT
  (payload->>'type') AS event_type,
  count(*)           AS cnt,
  round(avg(((payload->>'score')::numeric))::numeric, 2) AS avg_score
FROM t_events
WHERE (payload->>'type') IN ('click','view','purchase')
GROUP BY (payload->>'type')
ORDER BY cnt DESC;

-- Text search-ish (not using extensions)
SELECT count(*) AS note_like_cnt
FROM t_events
WHERE note LIKE '%AAAA%';

-- ===========================
-- 7) “Top-N per group” pattern
-- ===========================
WITH topn AS (
  SELECT
    k, id, ts, v1,
    row_number() OVER (PARTITION BY k ORDER BY v1 DESC) AS rn
  FROM t_events
)
SELECT k, id, ts, v1
FROM topn
WHERE rn <= 3
ORDER BY k, rn;

-- ===========================
-- 8) Self-join / correlation work
-- ===========================
-- This can get expensive; keep it bounded via sample + key grouping
WITH sample AS (
  SELECT *
  FROM t_events
  WHERE id % 10 = 0   -- sample 10%
),
pairs AS (
  SELECT
    a.k,
    a.id AS id_a,
    b.id AS id_b,
    abs(a.v1 - b.v1) AS v1_diff
  FROM sample a
  JOIN sample b
    ON a.k = b.k
   AND a.id < b.id
  WHERE a.id % 1000 = 0  -- further bound
)
SELECT k, count(*) AS pair_cnt, avg(v1_diff) AS avg_diff
FROM pairs
GROUP BY k
ORDER BY pair_cnt DESC
LIMIT 50;

-- ===========================
-- 9) Cleanup summary
-- ===========================
SELECT
  pg_size_pretty(pg_total_relation_size('t_events')) AS t_events_size,
  pg_size_pretty(pg_total_relation_size('t_dim'))    AS t_dim_size;

-- End of script (TEMP tables auto-drop on commit end, but editor may autocommit)

