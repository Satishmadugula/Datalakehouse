# Troubleshooting

## Common issues

### Docker compose services restart repeatedly

* Run `docker compose -f docker/docker-compose.yml ps` to inspect health statuses.
* Ensure ports 9000/9001 (MinIO), 5432 (Postgres), 19120 (Nessie), 9047 (Dremio) are free.
* If MinIO fails to create buckets, rerun `docker/minio/create-buckets.sh` manually once the container is healthy.

### Airflow DAGs stuck in queued

* Verify the scheduler is running: `docker compose logs airflow-scheduler`.
* Check connection IDs in Airflow UI under Admin → Connections; the bootstrap script creates `minio_default`, `spark_default`, `nessie_default`, `dremio_default`.
* Ensure Spark service is reachable at `spark://spark:7077` (see `docker/docker-compose.yml`).

### NiFi processors invalid

* Import `config/nifi/controller_services.json` via NiFi Registry or REST API.
* Update controller services with the correct credentials from `.env` (especially MongoDB and MinIO access keys).
* Restart affected processors after enabling controller services.

### Spark job fails with `ClassNotFoundException: org.apache.iceberg`.

* The Spark Dockerfile downloads Iceberg 1.5.x JARs during build. Rebuild the image: `docker compose build spark`.
* Ensure the job includes `--jars /opt/spark/jars/*` or `spark.jars` property (already set in `spark-defaults.conf`).

### Kafka topics missing (when enabled)

* Run `docker exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list`.
* If no topics exist, apply `config/kafka/topics.json` using `scripts/bootstrap_local.sh`.

### Dremio sources not visible

* Check Dremio logs in `docker compose logs dremio`.
* Delete bootstrap marker file `/opt/dremio/bootstrapped` and restart container to rerun bootstrap.

## Log locations

| Service | Location |
|---------|----------|
| Airflow | `docker/airflow/logs` volume |
| Spark jobs | `/opt/spark/work-dir/logs` inside container |
| NiFi | `/opt/nifi/logs` |
| Nessie | `/var/log/nessie` |
| Dremio | `/opt/dremio/log` |

## Health checks

* MinIO: `curl -I http://localhost:9000/minio/health/live`
* Nessie: `curl http://localhost:19120/api/v2/config`
* Airflow: `curl http://localhost:8080/health`
* NiFi: `curl http://localhost:9090/nifi-api/system-diagnostics`

## Resetting the environment

1. `make down`
2. Remove volumes: `docker volume prune`
3. Remove generated data: `rm -rf data/minio data/postgres`
4. Re-run `make up` followed by `scripts/bootstrap_local.sh`

