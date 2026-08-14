-- Unity Catalog objects required by the NASA GCN Fermi pipeline.
-- Run with a principal that has permission to create catalogs and schemas.

CREATE CATALOG IF NOT EXISTS kafka_nasa_fermi_topic
COMMENT 'NASA GCN Fermi streaming lakehouse';

CREATE SCHEMA IF NOT EXISTS kafka_nasa_fermi_topic.bronze
COMMENT 'Raw Kafka payloads and delivery metadata';

CREATE SCHEMA IF NOT EXISTS kafka_nasa_fermi_topic.silver
COMMENT 'Parsed and cleaned NASA GCN notices';

CREATE SCHEMA IF NOT EXISTS kafka_nasa_fermi_topic.gold
COMMENT 'Analytics-ready GCN fact and dimension tables';
