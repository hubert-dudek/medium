-- Databricks notebook source
CREATE OR REPLACE TABLE workspace.default.news_20261021_transcription AS
SELECT ai_transcribe(content) AS result
FROM read_files(
  '/Volumes/workspace/default/news_20261021_audio/news_sample.wav',
  format => 'binaryFile'
);

-- COMMAND ----------

SELECT
  segment.value:start::DOUBLE AS start_seconds,
  segment.value:speaker_id::STRING AS speaker,
  segment.value:text::STRING AS text
FROM workspace.default.news_20261021_transcription,
LATERAL variant_explode(result:response.segments) AS segment;
