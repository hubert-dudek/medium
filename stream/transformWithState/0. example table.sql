-- Databricks notebook source
CREATE OR REPLACE TABLE default.events (
  id BIGINT,
  data STRING,
  status STRING,
  update_time TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  delta.appendOnly = true,
  delta.enableChangeDataFeed = false
);

-- COMMAND ----------

INSERT INTO default.events (id, data, status, update_time) VALUES
('1', 'data1', 'active', '2025-03-04 10:00:00'),
('2', 'data2', 'inactive', '2025-03-04 11:00:00'),
('3', 'data3', 'active', '2025-03-04 12:00:00'),
('3', 'data3', 'active', '2025-03-04 12:00:00')
;

-- COMMAND ----------

INSERT INTO default.events (id, data, status, update_time) VALUES
('1', 'data1', 'active', '2025-03-04 10:00:00'),
('2', 'data2', 'inactive', '2025-03-04 11:00:00'),
('4', 'data2', 'inactive', '2025-03-04 11:00:00')
;

-- COMMAND ----------

CREATE OR REPLACE TABLE default.hisotrical_events (
  id BIGINT,
  data STRING,
  status STRING,
  update_time TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  delta.appendOnly = true,
  delta.enableChangeDataFeed = false
);

-- COMMAND ----------

INSERT INTO default.events (id, data, status, update_time) VALUES
('1', 'data1', 'active', '2024-03-04 10:00:00'),
('2', 'data2', 'inactive', '2024-03-04 11:00:00'),
('3', 'data3', 'active', '2024-03-04 12:00:00')
;
