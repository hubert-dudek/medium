# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
import pandas as pd
from typing import Iterator

historical_df = spark.read.format("delta").table("default.historical_events")

output_schema = StructType([
    StructField("id", StringType(), True),
    StructField("total_count", LongType(), True)
])

class BackfillProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("count", LongType(), True)])
        self.count_state = handle.getValueState("count_state", state_schema)
    
    def handleInputRows(self, key, rows: Iterator[pd.DataFrame], timer_values) -> Iterator[pd.DataFrame]:
        current_count = self.count_state.get()[0] if self.count_state.exists() else 0
        batch_count = sum(len(pdf) for pdf in rows)
        new_total = current_count + batch_count
        self.count_state.update((new_total,))
        yield pd.DataFrame({"id": [key], "total_count": [new_total]})

df = spark.readStream.format("delta").table("default.events")

backfill_stream = (
    df.groupBy("id")
      .transformWithStateInPandas(
          statefulProcessor=BackfillProcessor(),
          outputStructType=output_schema,
          outputMode="Update",
          timeMode="None",
          initialState=historical_df
      )
)

