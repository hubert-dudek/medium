# IPython log file

from prewarm_jedi_cache import prewarm_jedi_cache
prewarm_jedi_cache("asyncio,botocore,ipywidgets,numpy,py4j,pyarrow,pyspark,pyspark.ml,pyspark.sql,sklearn,spark")
del prewarm_jedi_cache
print(1)
get_ipython().run_cell_magic('writefile', 'test.txt', 'bla bla\n')
get_ipython().run_line_magic('cat', 'test.txt')
# -- This is a system generated query for notebook serverless keepalive
pass
get_ipython().run_cell_magic('timeit', '20', 'df.selectExpr("id * 2").count()\n')
df = spark.createDataFrame([(1, 'a'), (2, 'b')], ['id', 'value'])
df.show()
get_ipython().run_cell_magic('timeit', '20', 'df.selectExpr("id * 2").count()\n')
# -- This is a system generated query for notebook serverless keepalive
pass
# -- This is a system generated query for notebook serverless keepalive
pass
# -- This is a system generated query for notebook serverless keepalive
pass
# -- This is a system generated query for notebook serverless keepalive
pass
# -- This is a system generated query for notebook serverless keepalive
pass
# -- This is a system generated query for notebook serverless keepalive
pass
# -- This is a system generated query for notebook serverless keepalive
pass
# -- This is a system generated query for notebook serverless keepalive
pass
# -- This is a system generated query for notebook serverless keepalive
pass
# -- This is a system generated query for notebook serverless keepalive
pass
# -- This is a system generated query for notebook serverless keepalive
pass
# -- This is a system generated query for notebook serverless keepalive
pass
# -- This is a system generated query for notebook serverless keepalive
pass
# -- This is a system generated query for notebook serverless keepalive
pass
# -- This is a system generated query for notebook serverless keepalive
pass
# -- This is a system generated query for notebook serverless keepalive
pass
# -- This is a system generated query for notebook serverless keepalive
pass
# Sun, 04 Jan 2026 19:40:17
# -- This is a system generated query for notebook serverless keepalive
pass
# Sun, 04 Jan 2026 19:40:22
spark.range(2)
#[Out]# DataFrame[id: bigint]
