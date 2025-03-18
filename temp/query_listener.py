from pyspark.sql.streaming import StreamingQueryListener
import threading
import time

class InactivityListener(StreamingQueryListener):
    def __init__(self, query):
        self.last_batch_time = time.time()
        self.inactivity_timeout = 360
        self.query = query
        self.lock = threading.Lock()

    def onQueryStarted(self, event):
        print(f"Query started at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}: {event.id}")

    def onQueryProgress(self, event):
        with self.lock:
            if event.progress.numInputRows > 0:
                self.last_batch_time = time.time()

    def onQueryTerminated(self, event):
        print(f"Query terminated at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}: {event.id}")

    def check_inactivity(self):
        while self.query.isActive:
            with self.lock:
                if time.time() - self.last_batch_time > self.inactivity_timeout:
                    print(f"No new data detected. Stopping the query at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}.")
                    self.query.stop()
            time.sleep(1)


df_stream = (
    spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .table("system.billing.usage")
    .filter("_change_type IN ('insert', 'update_postimage')")
)


query = (
    df_stream.writeStream.format("delta")
    .option("checkpointLocation", CHECKPOINT)
    .foreachBatch(upsertToDelta)
    .start()
)

# Attach the listener
listener = InactivityListener(query)
spark.streams.addListener(listener)

# Start the inactivity check in a separate thread
thread = threading.Thread(target=listener.check_inactivity)
thread.start()

# Block until the query stops
query.awaitTermination()