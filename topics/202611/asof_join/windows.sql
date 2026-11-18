-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW window_events(event_id, customer_id, event_time) AS
VALUES
  (201, 1, TIMESTAMP '2026-11-18 09:45:00'),
  (202, 1, TIMESTAMP '2026-11-18 10:00:00'),
  (203, 1, TIMESTAMP '2026-11-18 11:15:00'),
  (204, 2, TIMESTAMP '2026-11-18 10:15:00'),
  (205, 2, TIMESTAMP '2026-11-18 10:45:00');

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW risk_windows(
  customer_id, window_id, valid_from, valid_to, risk_score
) AS
VALUES
  (1, 'early', TIMESTAMP '2026-11-18 09:00:00', TIMESTAMP '2026-11-18 10:00:00', 20),
  (1, 'current', TIMESTAMP '2026-11-18 10:00:00', TIMESTAMP '2026-11-18 11:00:00', 70),
  (1, 'later', TIMESTAMP '2026-11-18 11:30:00', TIMESTAMP '2026-11-18 12:00:00', 90),
  (2, 'wide', TIMESTAMP '2026-11-18 09:00:00', TIMESTAMP '2026-11-18 12:00:00', 30),
  (2, 'short', TIMESTAMP '2026-11-18 10:00:00', TIMESTAMP '2026-11-18 10:30:00', 80);

-- COMMAND ----------

SET spark.sql.join.asofJoin.enabled=true;

-- COMMAND ----------

SELECT e.event_id, v.window_id, v.risk_score
FROM window_events e
LEFT ASOF JOIN risk_windows v
  MATCH_CONDITION (e.event_time >= v.valid_from)
  ON e.customer_id = v.customer_id
  AND e.event_time < v.valid_to
ORDER BY e.event_id;
