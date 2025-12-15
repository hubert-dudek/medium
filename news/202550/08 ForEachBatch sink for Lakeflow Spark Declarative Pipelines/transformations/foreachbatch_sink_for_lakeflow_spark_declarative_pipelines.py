from pyspark import pipelines as dp

@dp.foreach_batch_sink(name="audit_sink")
def audit_sink(df, batch_id: int):


    (df.write
        .format("jdbc")
        .option("databricks.connection", "my_lakebase_pg_conn")
        .option("dbtable", "public.events_audited")
        .save())


@dp.table(name="silver_events")
def silver_events():
    return spark.readStream.table("hub.default.bronze_events")

@dp.append_flow(target="audit_sink", name="events_flow")
def events_flow():
    return spark.readStream.table("silver_events")
