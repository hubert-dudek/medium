CREATE TABLE IF NOT EXISTS hub.default.bronze_events (
  event_id        BIGINT,
  event_type      STRING,
  user_id         STRING,
  event_ts        TIMESTAMP
);

INSERT INTO hub.default.bronze_events VALUES
  (1, 'page_view',  'user_001', TIMESTAMP '2025-12-15 10:00:00'),
  (2, 'click',      'user_002', TIMESTAMP '2025-12-15 10:01:10'),
  (3, 'purchase',   'user_001', TIMESTAMP '2025-12-15 10:02:45'),
  (4, 'page_view',  'user_003', TIMESTAMP '2025-12-15 10:03:30'),
  (5, 'click',      'user_002', TIMESTAMP '2025-12-15 10:01:10');

  CREATE TABLE IF NOT EXISTS hub.default.events_clicks (
  event_type      STRING,
  clicks          BIGINT
);
