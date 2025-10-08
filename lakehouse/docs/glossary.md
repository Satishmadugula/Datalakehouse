# Glossary

| Term | Definition |
|------|------------|
| **Lakehouse** | Architecture combining data lake storage with data warehouse management features. |
| **Bronze/Silver/Gold** | Data refinement zones: bronze (raw structured), silver (cleaned/conformed), gold (curated for analytics). |
| **Project Nessie** | Git-like metadata catalog for Apache Iceberg. Manages branches, tags, and commits. |
| **Apache Iceberg** | Table format for huge analytic datasets providing ACID transactions, schema evolution, and time travel. |
| **Change Data Capture (CDC)** | Process of capturing and applying data changes from a source system in near real-time. |
| **NiFi** | Flow-based programming tool for data movement with back pressure, provenance, and scheduling. |
| **Airflow DAG** | Directed acyclic graph defining workflows and task dependencies in Apache Airflow. |
| **Reflection (Dremio)** | Pre-computed accelerations similar to materialized views used to speed up queries. |
| **Time travel** | Querying historical snapshots of data using snapshot IDs or timestamps. |
| **_rest column** | Map column storing unmodeled attributes for schema evolution safety. |
| **Compaction** | Process of rewriting many small data files into larger files for optimal query performance. |
| **Manifest** | Iceberg metadata file referencing data files with statistics. |
| **Watermark** | High-water mark used for idempotent ingestion runs. |
| **Schema evolution** | Controlled process to modify table schema over time while preserving historical reads. |

