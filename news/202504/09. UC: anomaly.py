# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.samples.transactions (
# MAGIC   transaction_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
# MAGIC   transaction_time TIMESTAMP,
# MAGIC   amount DECIMAL(10, 2)
# MAGIC )
# MAGIC TBLPROPERTIES ('delta.enableChangeDataCapture' = true);

# COMMAND ----------

from datetime import datetime, timedelta
import time

# Calculate the end time
end_time = datetime.now() + timedelta(minutes=10)

# Run the loop until the end time is reached
while datetime.now() < end_time:
    # Insert values using SQL
    spark.sql("""
        INSERT INTO course.quality_test.transactions (transaction_time, amount) VALUES
        (CURRENT_TIMESTAMP(), 100.00),
        (CURRENT_TIMESTAMP(), 150.50),
        (CURRENT_TIMESTAMP(), 200.75)
    """)
    
    # Sleep for 1 minute
    time.sleep(60)
