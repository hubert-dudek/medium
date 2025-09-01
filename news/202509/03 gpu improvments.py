# Databricks notebook source
# MAGIC %md
# MAGIC Schedule

# COMMAND ----------

# Import the distributed decorator
from serverless_gpu import distributed

# Decorate the function with @distributed and specify the number of GPUs, the GPU type, and whether or not the GPUs are remote
@distributed(gpus=2, gpu_type='A10', remote=True)
def hello_world(s: str) -> str:
  return 'hello_world'

# Trigger the distributed execution of the hello_world function
hello_world.distributed(s='abc')
