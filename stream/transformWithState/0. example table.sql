-- Databricks notebook source
DROP TABLE default.events;
DROP TABLE default.historical_events;

-- COMMAND ----------

CREATE OR REPLACE TABLE default.events (
  id BIGINT,
  data STRING,
  status STRING,
  update_time TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  delta.appendOnly = true
);

-- COMMAND ----------

INSERT INTO default.events (id, data, status, update_time) VALUES
('3', 'data3', 'active', '2024-03-04 12:00:00'),
('4', 'data1', 'active', '2025-03-04 10:00:00'),
('5', 'data2', 'inactive', '2025-03-04 11:00:00'),
('6', 'data3', 'active', '2025-03-04 12:00:00'),
('6', 'data3', 'active', '2025-03-04 12:00:00')
;

-- COMMAND ----------

CREATE OR REPLACE TABLE default.historical_events (
  id BIGINT,
  data STRING,
  status STRING,
  update_time TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  delta.appendOnly = true
);

-- COMMAND ----------

INSERT INTO default.historical_events (id, data, status, update_time) VALUES
('1', 'data1', 'active', '2024-03-04 10:00:00'),
('2', 'data2', 'inactive', '2024-03-04 11:00:00'),
('3', 'data3', 'active', '2024-03-04 12:00:00'),
('4', 'data1', 'active', '2025-03-04 10:00:00');
