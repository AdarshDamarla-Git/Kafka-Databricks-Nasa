# Databricks notebook source
client_id = dbutils.secrets.get(
    scope="nasa-gcn",
    key="client-id"
)

client_secret = dbutils.secrets.get(
    scope="nasa-gcn",
    key="client-secret"
)

# COMMAND ----------


from pyspark.sql.functions import col

spark.readStream.format("kafka") \
            .option(
                "kafka.bootstrap.servers",
                "kafka.gcn.nasa.gov:9092"
            ) \
            .option(
                "kafka.security.protocol",
                "SASL_SSL"
            ) \
            .option(
                "kafka.sasl.mechanism",
                "OAUTHBEARER"
            ) \
            .option(
                "kafka.sasl.jaas.config",
                "kafkashaded.org.apache.kafka.common.security.oauthbearer."
                "OAuthBearerLoginModule required "
                f'clientId="{client_id}" '
                f'clientSecret="{client_secret}";'
            ) \
            .option(
                "kafka.sasl.login.callback.handler.class",
                "kafkashaded.org.apache.kafka.common.security.oauthbearer."
                "OAuthBearerLoginCallbackHandler"
            ) \
            .option(
                "kafka.sasl.oauthbearer.token.endpoint.url",
                "https://auth.gcn.nasa.gov/oauth2/token"
            ) \
            .option(
                "subscribe",
                "gcn.classic.text.FERMI_GBM_FIN_POS"
            ) \
            .option(
                "startingOffsets",
                "earliest"
            ) \
            .option(
                "failOnDataLoss",
                "false"
            ) \
            .load() \
            .select(
                col("key").cast("string").alias("key"),
                col("value").cast("string").alias("value"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp")
            )
