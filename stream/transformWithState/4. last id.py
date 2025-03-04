# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
import pandas as pd
from typing import Iterator
import threading

# We'll store the latest record for each key in a dictionary (in the driver).
# This is not recommended for production scale, but shows how one might do it for demonstration.
latest_records_dict = {}
lock = threading.Lock()

output_schema = StructType([
    StructField("id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("update_time", TimestampType(), True)
])

class LastRecordProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([
            StructField("id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("update_time", TimestampType(), True)
        ])
        self.last_record = handle.getValueState("last_record", state_schema)
    
    def handleInputRows(self, key, rows: Iterator[pd.DataFrame], timer_values) -> Iterator[pd.DataFrame]:
        batch_df = pd.concat(list(rows), ignore_index=True)
        # For simplicity, just assume there's only one record or take the last
        # Typically you'd do a sort by timestamp if there are multiple.
        row = batch_df.iloc[-1]
        
        # Update the state with this row
        self.last_record.update((row["id"], row["status"], row["update_time"]))
        
        # Also update a driver-level dictionary (demo purposes)
        with lock:
            latest_records_dict[row["id"]] = {
                "status": row["status"],
                "update_time": row["update_time"]
            }
        
        # Emit the row
        yield batch_df

df = spark.readStream.format("delta").table("main.analytics.events")

latest_stream = (
    df.groupBy("id")
      .transformWithStateInPandas(
          statefulProcessor=LastRecordProcessor(),
          outputStructType=output_schema,
          outputMode="Append",
          timeMode="None"
      )
)

# Now 'latest_records_dict' (in the driver) will hold the latest status & time for each 'id' that came through.
# We can read it in interactive mode, but it won't necessarily be up-to-the-minute if the stream is distributed.

