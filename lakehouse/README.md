# Lakehouse Reference Implementation

This repository provides an end-to-end, production-grade lakehouse reference architecture for MongoDB sources backed by Apache Iceberg on MinIO, orchestrated with Apache Airflow and Apache NiFi, cataloged by Project Nessie, and queried through Dremio Community Edition. Optional components such as Apache Kafka enable change data capture fan-out. The stack is fully reproducible using Docker Compose for local development and Helm for Kubernetes deployments.

## Quick start

### Prerequisites

* Docker 24+
* Docker Compose v2+
* Python 3.10+
* `jq`
* At least 16 GB of RAM and 4 CPU cores available to containers

### One-command bootstrap

```bash
cd lakehouse
cp env/.env.example .env
make up
scripts/bootstrap_local.sh
```

Once the bootstrap script completes, access the UIs:

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO | http://localhost:9001 | `admin` / `password` (change via env) |
| Airflow | http://localhost:8080 | `airflow` / `airflow` |
| NiFi | http://localhost:9090 | `nifi` / `nifi` |
| Dremio | http://localhost:9047 | `dremio` / `dremio` |
| Nessie API | http://localhost:19120 | no auth locally |
| Mongo Express | http://localhost:8081 | (if enabled) |

Run the smoke test end-to-end:

```bash
scripts/smoke_test.sh
```

The smoke test performs the following:

1. Seeds MongoDB with nested sample documents.
2. Triggers the NiFi batch flow to land JSON snapshots in MinIO `raw/`.
3. Runs the Spark batch job via Airflow to create/update Iceberg tables in the `payments` namespace.
4. Starts the Kafka-backed CDC stream, applies inserts/updates/deletes into Iceberg using structured streaming, and validates results.
5. Executes compaction and retention maintenance.
6. Queries Dremio to validate time travel reads.

### Common operations

```bash
make run-batch      # Trigger the batch ingestion Spark job
make run-stream     # Start the CDC streaming job in foreground
make compaction     # Execute compaction maintenance Spark job
make seed-mongo     # Generate synthetic data into Mongo
make dremio-login   # Print Dremio credentials and login URL
make down           # Tear down docker compose stack
```

Refer to [docs/architecture.md](docs/architecture.md) for system design, [docs/operations.md](docs/operations.md) for day 2 guidance, and [docs/troubleshooting.md](docs/troubleshooting.md) for help.

## Repository layout

```
lakehouse/
  docker/               # Docker compose and service images
  helm/                 # Kubernetes Helm charts
  airflow_contrib/      # Custom Airflow plugins/operators
  scripts/              # Operational helpers
  config/               # Shared configuration
  docs/                 # Architecture, operations, security docs
  tests/                # Pytest suites for automated validation
```

## Data model & namespaces

* **Catalog**: `lakehouse_catalog` (Project Nessie)
* **Iceberg namespace**: configurable, defaults to `payments`
* **Buckets**: `raw`, `bronze`, `silver`, `gold`, `iceberg-metadata`
* **Example tables**:
  * `payments.merchant_events` — CDC table with hash partition + daily partition
  * `payments.orders` — Batch-loaded snapshot table partitioned by event date
  * `payments.clickstream` — Schema evolution demo with nested JSON and `_rest` map

Partitioning defaults to `days(ts)` and optionally `bucket(16, merchant_id)` for CDC tables.

## Testing

Run automated tests after bootstrapping the stack:

```bash
pip install -r docker/airflow/requirements.txt
pytest tests -s
```

Tests assume services are running (via Docker Compose) and validate batch ingestion, CDC, time-travel queries, and compaction effectiveness.

## Contributions

This project is open-source under the Apache 2.0 license. Contributions via issues and pull requests are welcome.

