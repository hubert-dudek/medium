-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW state_progress AS
SELECT
  timestamp,
  origin.update_id,
  from_json(
    details:stream_progress.progress_json,
    'id STRING, runId STRING, durationMs MAP<STRING, BIGINT>,
     stateOperators ARRAY<STRUCT<
       numShufflePartitions: BIGINT,
       numRowsTotal: BIGINT,
       customMetrics: MAP<STRING, BIGINT>
     >>'
  ) AS progress
FROM event_log(TABLE(main.state_repartitioning_demo.pipeline_unique_events))
WHERE event_type = 'stream_progress';

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW resize_results AS
SELECT
  ROW_NUMBER() OVER (ORDER BY MIN(timestamp)) AS phase,
  MAX_BY(progress.stateOperators[0].numShufflePartitions, timestamp) AS partitions,
  MAX_BY(progress.stateOperators[0].numRowsTotal, timestamp) AS state_rows,
  SUM(progress.stateOperators[0].customMetrics['numDroppedDuplicateRows']) AS duplicates_dropped,
  SUM(progress.durationMs['controlBatch.REPARTITION']) AS repartition_ms
FROM state_progress
GROUP BY update_id;

-- COMMAND ----------

SELECT
  phase,
  partitions,
  state_rows,
  duplicates_dropped,
  repartition_ms
FROM resize_results
ORDER BY phase;

-- COMMAND ----------

SELECT
  COUNT(*) AS output_rows,
  COUNT(DISTINCT event_id) AS unique_events,
  MIN(event_id) AS first_event,
  MAX(event_id) AS last_event
FROM main.state_repartitioning_demo.pipeline_unique_events;
