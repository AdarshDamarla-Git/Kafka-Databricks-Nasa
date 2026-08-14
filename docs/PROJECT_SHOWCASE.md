# Project showcase

## One-line summary

Built an OAuth-authenticated Spark streaming lakehouse that consumes NASA GCN Fermi gamma-ray burst notices from Kafka, parses classic-text messages, and publishes a governed analytical snowflake schema.

## Portfolio description

This project ingests real-time Fermi GBM Final Position notices from NASA's General Coordinates Network. Databricks Structured Streaming connects to NASA's Kafka service through SASL/SSL and OAuth bearer authentication, preserving source offsets and timestamps in Bronze. Silver transforms semi-structured classic-text notices into 24 named fields using native Spark expressions. Gold casts scientific measurements and publishes a central GCN notice fact with timing, spatial, and Sun/Moon dimensions under Unity Catalog.

## Resume bullets

- Engineered a real-time Databricks pipeline that consumes NASA GCN Fermi GBM notices from Kafka using SASL/SSL and OAuth 2.0 authentication.
- Parsed multiline classic-text astronomy messages into 24 structured attributes with native PySpark transformations and no Python UDFs.
- Preserved Kafka topic, partition, offset, and timestamps for replay analysis, lineage, and delivery troubleshooting.
- Designed a governed Gold snowflake schema with a notice fact, three dimensions, hash surrogate keys, and informational Unity Catalog constraints.

## Interview talking points

### Why retain Kafka metadata?

Topic, partition, and offset identify the exact source position of every message. They support lineage, duplicate detection, offset-gap monitoring, and replay investigations.

### Why avoid a Python UDF for parsing?

Native Spark expressions remain visible to the query optimizer, avoid Python serialization overhead, and integrate cleanly with streaming execution.

### Why use a snowflake dimension?

Solar and lunar context can repeat across spatial records. Separating it makes the relationship explicit and provides a reusable astronomical-context entity.

### What would you improve next?

Add parse-quality expectations and quarantine handling, convert raw astronomy fields to typed values, widen surrogate hashes, monitor Kafka lag and offset gaps, and deploy with a Databricks Asset Bundle.

## Suggested GitHub topics

```text
nasa
nasa-gcn
fermi-gbm
gamma-ray-bursts
apache-kafka
pyspark
spark-structured-streaming
databricks
lakeflow
delta-lake
unity-catalog
oauth2
data-engineering
real-time-data
```

## Suggested repository description

Real-time NASA GCN Fermi pipeline using OAuth-authenticated Kafka, Spark Structured Streaming, Databricks Lakeflow, Delta Lake, and a Gold snowflake schema.
