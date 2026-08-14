# Architecture

## System context

```mermaid
flowchart LR
    GCN["NASA General Coordinates Network"]
    PIPE["Databricks streaming lakehouse"]
    USER["Astronomer · data engineer · analyst"]

    GCN -->|"Fermi GBM final-position notices"| PIPE
    USER -->|"operates and monitors"| PIPE
    PIPE -->|"structured notice facts and dimensions"| USER
```

## Component architecture

```mermaid
flowchart TB
    subgraph NASA["NASA GCN"]
        OAUTH["OAuth token endpoint"]
        BROKER["Kafka broker<br/>kafka.gcn.nasa.gov:9092"]
        TOPIC["gcn.classic.text<br/>FERMI_GBM_FIN_POS"]
        BROKER --- TOPIC
    end

    subgraph DBX["Databricks"]
        SECRETS["Secret scope: nasa-gcn"]
        SPARK["Spark Structured Streaming<br/>Kafka connector"]
        BRONZE["Bronze raw table"]
        SILVER["Silver parsed OBT"]
        GOLD["Gold fact + dimensions"]
        SQL["Databricks SQL / BI"]

        SECRETS --> SPARK
        SPARK --> BRONZE --> SILVER --> GOLD --> SQL
    end

    SECRETS --> OAUTH
    OAUTH --> BROKER
    TOPIC -->|"SASL_SSL / OAUTHBEARER"| SPARK
```

## Authentication and ingestion sequence

```mermaid
sequenceDiagram
    autonumber
    participant P as Lakeflow pipeline
    participant S as Databricks secret scope
    participant A as NASA OAuth endpoint
    participant K as NASA GCN Kafka
    participant B as Bronze table

    P->>S: Get client-id and client-secret
    S-->>P: Return secret values
    P->>A: Request OAuth bearer token
    A-->>P: Return access token
    P->>K: Connect with SASL_SSL / OAUTHBEARER
    P->>K: Subscribe to FERMI_GBM_FIN_POS
    loop Streaming micro-batches
        K-->>P: Key, value, topic, partition, offset, timestamp
        P->>B: Append cast payload and metadata
    end
```

## Transformation lineage

```mermaid
flowchart LR
    K["Kafka message"] --> B["FERMI_topic_raw"]
    B --> N["Normalize spaces"]
    N --> W["Merge wrapped lines"]
    W --> C["Consolidate COMMENTS"]
    C --> M["str_to_map"]
    M --> S["FERMI_topic_cleaned_data_OBT"]
    S --> F["fact_gcn_notices"]
    S --> T["dim_event_timing"]
    S --> P["dim_spatial_coords"]
    S --> SM["dim_sun_moon_coords"]
    T --> F
    P --> F
    SM --> P
```

## Medallion responsibilities

| Layer | Responsibility | Recovery value |
|---|---|---|
| Bronze | Preserve source payload and Kafka delivery coordinates | Enables replay analysis and parser debugging |
| Silver | Convert semi-structured classic text into named columns | Provides a reusable, queryable notice contract |
| Gold | Cast measurements and organize analytical relationships | Supports efficient SQL and BI consumption |

## Streaming semantics

| Setting or behavior | Meaning |
|---|---|
| `startingOffsets=earliest` | A new checkpoint starts from the earliest offsets still retained by Kafka |
| `failOnDataLoss=false` | The stream continues when expected offsets are unavailable |
| Kafka metadata retained | Each fact can be traced to its topic, partition, and offset |
| Streaming dimensions | New coordinate/time combinations are incrementally added |
| `dropDuplicates` | Repeated dimension surrogate keys are suppressed using streaming state |

## Failure boundaries

- Secret lookup or OAuth failure prevents the Bronze stream from starting.
- Kafka connectivity or topic authorization failure blocks ingestion.
- Unexpected classic-text formatting can produce missing Silver fields without necessarily failing the stream.
- Numeric strings that do not begin with parseable numbers become null in Gold measurement columns.
- Missing or inconsistent dimension keys can yield unmatched analytical joins because `RELY` constraints are informational.
