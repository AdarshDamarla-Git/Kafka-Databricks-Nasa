# Deployment and operations

## Deployment order

1. Register for NASA GCN access and create OAuth client credentials.
2. Create the Unity Catalog catalog and Bronze, Silver, and Gold schemas.
3. Create the `nasa-gcn` Databricks secret scope.
4. Add `client-id` and `client-secret` to the scope.
5. Confirm Databricks compute can reach `kafka.gcn.nasa.gov:9092` and `auth.gcn.nasa.gov`.
6. Create a Lakeflow pipeline with the three transformation files.
7. Ensure the utilities module is importable by the Gold transformation.
8. Start an update and validate all tables.
9. Configure monitoring for pipeline failures, offsets, lag, and parse quality.

## Setup SQL

The package includes [`setup/CREATE_CATALOG.sql`](../setup/CREATE_CATALOG.sql) for the initial Unity Catalog objects.

## Secrets

Required scope and keys:

```text
scope: nasa-gcn
keys:
  - client-id
  - client-secret
```

Use your organization's approved secret-management workflow. Never print retrieved secrets in notebooks or logs.

## Smoke tests

```sql
SELECT COUNT(*) FROM kafka_nasa_fermi_topic.bronze.FERMI_topic_raw;
SELECT COUNT(*) FROM kafka_nasa_fermi_topic.silver.FERMI_topic_cleaned_data_OBT;
SELECT COUNT(*) FROM kafka_nasa_fermi_topic.gold.fact_gcn_notices;
```

```sql
SELECT
  kafka_partition,
  MAX(kafka_offset) AS latest_offset,
  MAX(kafka_timestamp) AS latest_timestamp
FROM kafka_nasa_fermi_topic.bronze.FERMI_topic_raw
GROUP BY kafka_partition;
```

## Monitoring checklist

- OAuth/token-refresh failures
- Kafka connectivity and authorization failures
- Consumer lag and stalled offsets
- Gaps in expected offset sequences
- Null or malformed `kafka_value` payloads
- Silver parse-null percentage by field
- Duplicate topic/partition/offset combinations
- Dimension hash collisions or unexpected key reuse
- Gold fact-to-dimension orphan counts
- Lakeflow event-log errors and update duration

## Triggered versus continuous mode

| Mode | Use when | Tradeoff |
|---|---|---|
| Continuous | Lowest-latency notice availability matters | Compute remains active and operational monitoring is continuous |
| Triggered | Periodic analytical refresh is sufficient | Lower idle cost but higher notice latency |

## Recovery notes

- Restarting with the same checkpoint resumes from committed offsets.
- A new checkpoint combined with `earliest` reprocesses offsets still retained by Kafka.
- Because `failOnDataLoss` is false, missing offsets may be skipped; alert on discontinuities if completeness is required.
- Avoid deleting checkpoints or performing full refreshes without evaluating replay, duplicates, state, and source retention.
