# Operations Guide

This guide covers day-2 operational procedures for the lakehouse platform. All commands assume the repository root `lakehouse/`.

## Service management

### Docker Compose

* Start: `make up`
* Stop: `make down`
* View logs: `docker compose -f docker/docker-compose.yml logs -f <service>`
* Scale NiFi or Spark workers: `docker compose up -d --scale nifi=2`

### Kubernetes (Helm)

1. Install the charts with shared values:

   ```bash
   helm repo add nessie https://projectnessie.org/helm
   kubectl create namespace lakehouse
   helm upgrade --install lakehouse ./helm -n lakehouse -f helm/values.yaml
   ```

2. Set secrets via Kubernetes secrets objects before the install. Required keys mirror `env/.env.example` (e.g., `minio-root-user`, `minio-root-password`).

3. Airflow runs with the `KubernetesExecutor`. Ensure a service account with permissions to create pods in the namespace. An example RBAC manifest is provided in `helm/charts/airflow/templates/rbac.yaml`.

## Backup & restore

| Component | Backup strategy | Restore |
|-----------|-----------------|---------|
| MinIO | Use `mc mirror` to replicate `raw`, `bronze`, `silver`, `gold`, `iceberg-metadata` buckets to cold storage. Enable object locking in production. | Mirror back and trigger Iceberg snapshot refresh. |
| Nessie (PostgreSQL) | Nightly `pg_dump` of `nessie` database. | `psql` restore followed by Iceberg metadata reconciliation. |
| Airflow metadata | Export via `pg_dump`. | Restore before upgrading Airflow schemas. |
| NiFi state | Export templates and flow registry snapshots. | Import via NiFi UI and restart processors. |

## Scaling considerations

* **NiFi**: Increase the number of nodes and enable a shared Zookeeper-based cluster for horizontal scale. Tune back-pressure thresholds per processor group.
* **Spark**: Switch to Spark-on-Kubernetes with dynamic allocation. Adjust `spark.executor.instances` via environment variables.
* **Dremio**: Configure separate coordinator/executor nodes. Increase heap size and reflection refresh cadence.
* **Kafka**: When enabled, scale brokers and partitions using `kafka-topics.sh`.

## Monitoring & observability

* **Metrics**: NiFi exposes Prometheus metrics on `/nifi-api/flow/status`. Airflow exposes statsd; configure `statsd_exporter` if needed. Spark history server is embedded in the image. Dremio offers JMX metrics.
* **Logging**: Log levels are controlled via env files (e.g., `SPARK_LOG_LEVEL`). Centralize logs by mounting volumes to an ELK stack.

## Security hardening

* Replace default credentials and propagate via environment files or Kubernetes secrets.
* Enable TLS with Traefik or Ingress NGINX. Terminate HTTPS at the ingress and forward to internal services. Documented below.
* Activate MinIO object locking and versioning for compliance.
* Configure Nessie authentication by enabling OAuth2 in `docker/nessie/application.properties` (instructions in comments).
* Integrate Apache Ranger or AWS Lake Formation for fine-grained authorization if required.

### TLS with Traefik/Ingress

1. Deploy Traefik or NGINX Ingress controller in the `lakehouse` namespace.
2. Create `Ingress` resources for Airflow, Dremio, NiFi, and MinIO, referencing TLS secrets created via cert-manager or manual certificates.
3. Update the Helm `values.yaml` to set `ingress.enabled=true` for each chart and provide hostnames (e.g., `airflow.lakehouse.example.com`).

## Disaster recovery runbook

1. Stop ingestion pipelines via Airflow DAG pause.
2. Snapshot MinIO buckets and database dumps.
3. Restore infrastructure in target region/cluster using Helm.
4. Restore PostgreSQL dumps and run `nessie --uri ... catalog-maintenance` to validate integrity.
5. Replay CDC events from Kafka if available; otherwise re-run batch snapshots.

## Nessie branch management

* Create development branches per feature: `nessie branch create feature/foo --from main`.
* Spark jobs can target branches by setting `NESSIE_REF`. Airflow DAG `maintenance_compaction_snapshot_policies.py` merges approved branches using `nessie_client` helper.
* Tags capture production releases (`nessie tag create prod-YYYYMMDD --ref main`).

