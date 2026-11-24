# Databricks notebook source
from pathlib import Path

audio = Path("news_sample.wav").read_bytes()
spark.createDataFrame([(audio,)], "content BINARY").createOrReplaceTempView("sample_audio")

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH transcript AS (
# MAGIC   SELECT ai_transcribe(content) AS result FROM sample_audio
# MAGIC )
# MAGIC SELECT
# MAGIC   segment.value:start::DOUBLE AS start_seconds,
# MAGIC   segment.value:speaker_id::STRING AS speaker,
# MAGIC   segment.value:text::STRING AS text,
# MAGIC   result:error_message::STRING AS error_message
# MAGIC FROM transcript,
# MAGIC LATERAL variant_explode_outer(result:response.segments) AS segment
# MAGIC ORDER BY start_seconds
