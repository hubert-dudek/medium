# Databricks notebook source
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
import pandas as pd
from typing import Iterator

# Output schema for the deduplicated records
output_schema = StructType(
    [StructField("id", LongType(), True), StructField("data", StringType(), True)]
)

class DeduplicateProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:

        self.seen_flag = handle.getValueState(
            "seen_flag", output_schema
        )  
        # schema can be diffrent we don't need to keep everything in store
        # third param is TTL in seconds

    def handleInputRows(
        self, key, rows: Iterator[pd.DataFrame], timer_values
    ) -> Iterator[pd.DataFrame]:
        
        # we loop all rows for given key in current micro-batch as it is grouping, we can implement some logic here
        for pdf in rows:
            for _, pd_row in pdf.iterrows():
                data = pd_row

        # we are checking is data exisitng for given Key (one from groupBY) in RocksDB
        if not self.seen_flag.exists():

            self.seen_flag.update((data[0],data[1])) # data which will be stored together with our key in RocksDB
            yield pd.DataFrame(
                {"id": key, "data": (data[1],)}
            ) # data which we return to browser

    def close(self):
        # Some DBR versions require close() with no argument
        pass


display(
    spark.readStream.table("default.events")
    .groupBy("id")
    .transformWithStateInPandas(
        statefulProcessor=DeduplicateProcessor(),
        outputStructType=output_schema,
        outputMode="Append",
        timeMode="None",
    )
)
