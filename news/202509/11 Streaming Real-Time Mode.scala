// Databricks notebook source
// MAGIC %md
// MAGIC spark.databricks.streaming.realTimeMode.enabled true

// COMMAND ----------

import org.apache.spark.sql.execution.streaming.RealTimeTrigger

val readStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("subscribe", inputTopic).load()
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("topic", outputTopic)
      .option("checkpointLocation", checkpointLocation)
      .outputMode(“update”)
      .trigger(RealTimeTrigger.apply())
      .start()

// COMMAND ----------

// MAGIC %md
// MAGIC 5 ms!

// COMMAND ----------

.trigger(RealTimeTrigger.apply("5 minutes"))

// COMMAND ----------

// MAGIC %md
// MAGIC OLTP
