from pyspark import pipelines as dp


@dp.foreach_batch_sink(name="audit_sink")
def audit_sink(df, batch_id: int):

    df.createOrReplaceTempView("df_view")
    df.sparkSession.sql(
        """
    MERGE INTO hub.default.events_clicks AS tgt
    USING (
      SELECT count(1) as clicks FROM
      df_view WHERE event_type = 'click') AS src
    ON tgt.event_type = 'click'
    WHEN MATCHED THEN UPDATE SET
      tgt.clicks = tgt.clicks + src.clicks
  """
    )
    return


@dp.table(name="silver_events")
def silver_events():
    return spark.readStream.table("hub.default.bronze_events")


@dp.append_flow(target="audit_sink", name="events_flow")
def events_flow():
    return spark.readStream.table("silver_events")
