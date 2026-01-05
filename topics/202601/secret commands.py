# Databricks notebook source
# MAGIC %md
# MAGIC This article is not about classic magic commands which we use for years like %sql or %run but about few more magic ommands which is good to know.

# COMMAND ----------

# MAGIC %lsmagic

# COMMAND ----------

# MAGIC %md
# MAGIC We have special databricks command like %sql and then we have line magic like %run and cel magic which applied to whole cell like %%writefile

# COMMAND ----------

# MAGIC %%writefile test.txt
# MAGIC bla bla

# COMMAND ----------

# MAGIC %cat test.txt

# COMMAND ----------

# MAGIC %%time
# MAGIC df = spark.range(10_000_000)
# MAGIC df.count()

# COMMAND ----------

# MAGIC %%timeit
# MAGIC spark.range(1_000_000).count()

# COMMAND ----------

# MAGIC %md
# MAGIC - Spark has warm-up cost
# MAGIC - Lazy evaluation
# MAGIC - Caching effects

# COMMAND ----------

# MAGIC %%capture out
# MAGIC df = spark.createDataFrame([(1, 'a'), (2, 'b')], ['id', 'value'])
# MAGIC df.show()

# COMMAND ----------

print(out.stdout)

# COMMAND ----------

# MAGIC %%sh
# MAGIC python --version

# COMMAND ----------

# MAGIC %%bash
# MAGIC ls /Workspace/

# COMMAND ----------

# MAGIC %md
# MAGIC For shell command we needto remember that we can use also a lot of shell command as magic commands which jsut exevcute she;; comand like %pwd %cs and other. Below is example of %ls

# COMMAND ----------

# MAGIC %ls /Workspace/

# COMMAND ----------

# MAGIC %%sx
# MAGIC ls /Workspace/

# COMMAND ----------

files = %sx ls /Workspace

# COMMAND ----------

print(files)

# COMMAND ----------

# DBTITLE 1,Python profile (not spark)
# MAGIC %%prun
# MAGIC def slow():
# MAGIC     total = 0
# MAGIC     for i in range(1_000_000):
# MAGIC         total += i
# MAGIC     return total
# MAGIC
# MAGIC slow()

# COMMAND ----------

# MAGIC %%debug
# MAGIC x = 1
# MAGIC y = 0
# MAGIC x / y

# COMMAND ----------

# MAGIC %skip
# MAGIC print("This cell will be skipped when running multiple cells")

# COMMAND ----------

# DBTITLE 1,Max cell output in mb - prevent sessio ncrash and storing too much info on storage
# MAGIC %set_cell_max_output_size_in_mb 1

# COMMAND ----------

# MAGIC %who
# MAGIC

# COMMAND ----------

# MAGIC %whos

# COMMAND ----------

# MAGIC %pinfo spark.range
# MAGIC

# COMMAND ----------

# MAGIC %history 

# COMMAND ----------

# MAGIC %logstart -o -t ipython_log.py
# MAGIC %logstate
# MAGIC print("this will be logged")

# COMMAND ----------

spark.range(2)

# COMMAND ----------

# MAGIC
# MAGIC %logstop

# COMMAND ----------

# MAGIC %env

# COMMAND ----------

# MAGIC %env MY_FLAG=1
# MAGIC %env MY_FLAG

# COMMAND ----------

# MAGIC %md
# MAGIC
