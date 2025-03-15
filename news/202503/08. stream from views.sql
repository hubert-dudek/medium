-- Databricks notebook source
CREATE TABLE demo_table (
    id INT,
    name STRING,
    timestamp TIMESTAMP
)

-- COMMAND ----------

CREATE OR REPLACE VIEW demo_view AS
SELECT * FROM demo_table;

-- COMMAND ----------

INSERT INTO demo_table VALUES
(1, 'Alice', current_timestamp()),
(2, 'Bob', current_timestamp()),
(3, 'Charlie', current_timestamp());


-- COMMAND ----------

-- MAGIC %python
-- MAGIC stream_df = spark.readStream.format("delta").table("demo_view")
-- MAGIC
-- MAGIC display(stream_df)
