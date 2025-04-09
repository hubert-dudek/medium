# Databricks notebook source
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
import pandas as pd
from typing import Iterator

historical_df = spark.read.format("delta").table("default.historical_events").groupBy("id")

output_schema = StructType(
    [StructField("id", LongType(), True), StructField("total_count", LongType(), True)]
)

class BackfillProcessor(StatefulProcessor):

    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("count", LongType(), True)])
        self.count_state = handle.getValueState("count_state", state_schema)

    def handleInitialState(self, key, initialStateRows, timerValues) -> None:
        # we will update state based on historical data, which is unique for every key
        if self.count_state.exists():
            return

        count = len(initialStateRows) # as it is just grouped pandas dataframe in that scenario we do len to count number of records
        self.count_state.update((count,))

    def handleInputRows(self, key, rows: Iterator[pd.DataFrame], timer_values) -> Iterator[pd.DataFrame]:
        # 2) Now handle streaming data
        current_count = 0 if not self.count_state.exists() else self.count_state.get()[0]
        # it is iterator so we loop over it
        for pdf in rows:
            for _, row in pdf.iterrows():
                current_count += 1
        self.count_state.update((current_count,))
        yield pd.DataFrame({"id": [key[0]], "total_count": [current_count]})

    def close(self):
        pass

df = spark.readStream.format("delta").table("default.events")

backfill_stream = df.groupBy("id").transformWithStateInPandas(
    statefulProcessor=BackfillProcessor(),
    outputStructType=output_schema,
    outputMode="Append",
    timeMode="None",
    initialState=historical_df
)

display(backfill_stream)
