client_id = dbutils.secrets.get(
    scope="nasa-gcn",
    key="client-id"
)

client_secret = dbutils.secrets.get(
    scope="nasa-gcn",
    key="client-secret"
)

from pyspark import pipelines as dp
from pyspark.sql.functions import col


@dp.table(
    name="kafka_nasa_fermi_topic.bronze.FERMI_topic_raw",
    comment="Raw NASA GCN Fermi GBM Final Position events from Kafka"
)
def fermi_topic_raw():

    return (
        spark.readStream
            .format("kafka")

            # Kafka connection
            .option(
                "kafka.bootstrap.servers",
                "kafka.gcn.nasa.gov:9092"
            )

            # Authentication
            .option(
                "kafka.security.protocol",
                "SASL_SSL"
            )
            .option(
                "kafka.sasl.mechanism",
                "OAUTHBEARER"
            )
            .option(
                "kafka.sasl.jaas.config",
                "kafkashaded.org.apache.kafka.common.security.oauthbearer."
                "OAuthBearerLoginModule required "
                f'clientId="{client_id}" '
                f'clientSecret="{client_secret}";'
            )
            .option(
                "kafka.sasl.login.callback.handler.class",
                "kafkashaded.org.apache.kafka.common.security.oauthbearer."
                "OAuthBearerLoginCallbackHandler"
            )
            .option(
                "kafka.sasl.oauthbearer.token.endpoint.url",
                "https://auth.gcn.nasa.gov/oauth2/token"
            )

            # Kafka topic
            .option(
                "subscribe",
                "gcn.classic.text.FERMI_GBM_FIN_POS"
            )

            .option(
                "startingOffsets",
                "earliest"
            )
            .option(
                "failOnDataLoss",
                "false"
            )

            .load()

            # Kafka binary -> string
            .select(
                col("key").cast("string").alias("kafka_key"),
                col("value").cast("string").alias("kafka_value"),
                col("topic").alias("kafka_topic"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset"),
                col("timestamp").alias("kafka_timestamp")
            )
    )