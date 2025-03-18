import dlt
from pyspark.sql.functions import *

# require EventHub standard, does not work with EventHub basic, send and listen policy

server = "trainingevents.servicebus.windows.net"

event_hub_connection_string = (
    f"Endpoint=sb://{server}/;"
    "SharedAccessKeyName=policy;"
    "SharedAccessKey=XXX"
)

kafka_sasl_jaas_config = (
    'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
    'username="$ConnectionString" '
    'password="{}";'
).format(event_hub_connection_string)

@dlt.table(table_properties={"pipelines.reset.allowed":"false"})
def transactions_bronze():
  return (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", f"{server}:9093")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config)
    .option("subscribe", f"transactions_{i}")
    .option("startingOffsets", "earliest")
    .load()
    )