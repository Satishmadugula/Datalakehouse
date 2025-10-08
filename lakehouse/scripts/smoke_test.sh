#!/bin/bash
set -euo pipefail
ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

docker compose -f docker/docker-compose.yml exec spark python /opt/spark/scripts/generate_test_data.py --uri "${MONGO_URI:-mongodb://mongo:mongo@mongo:27017/}"

docker compose -f docker/docker-compose.yml exec airflow-webserver airflow dags trigger ingest_batch_mongo_to_iceberg
sleep 30
docker compose -f docker/docker-compose.yml exec spark spark-submit /opt/spark/jobs/compaction_rewrite_data_files.py

docker compose -f docker/docker-compose.yml exec spark spark-submit \
  /opt/spark/jobs/stream_kafka_to_iceberg.py --enable-kafka --namespace "${ICEBERG_NAMESPACE:-payments}" --topic "${KAFKA_CDC_TOPIC_PREFIX:-mongo.cdc}.orders" || true

docker compose -f docker/docker-compose.yml exec spark python - <<'PY'
from pyiceberg.catalog import load_catalog
catalog = load_catalog("nessie", uri="${NESSIE_URI:-http://nessie:19120/api/v2}", warehouse="s3a://gold")
table = catalog.load_table("${ICEBERG_NAMESPACE:-payments}.orders")
print(f"Orders snapshot: {table.current_snapshot().snapshot_id}")
PY

echo "Smoke test completed"
