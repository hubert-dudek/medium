# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
import pandas as pd
from typing import Iterator

output_schema = StructType([
    StructField("id", StringType(), True),
    StructField("count", IntegerType(), True)
])

class EventCountProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("count", IntegerType(), True)])
        self.counter_state = handle.getValueState("counter", state_schema)
    
    def handleInputRows(self, key, rows: Iterator[pd.DataFrame], timer_values) -> Iterator[pd.DataFrame]:
        existing_count = self.counter_state.get()[0] if self.counter_state.exists() else 0
        new_events = sum(len(pdf) for pdf in rows)
        total = existing_count + new_events
        self.counter_state.update((total,))
        yield pd.DataFrame({"id": [key], "count": [total]})

df = spark.readStream.format("delta").table("main.analytics.events")

count_stream = (
    df.groupBy("id")
      .transformWithStateInPandas(
          statefulProcessor=EventCountProcessor(),
          outputStructType=output_schema,
          outputMode="Update",
          timeMode="None"
      )
)
