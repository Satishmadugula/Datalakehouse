#!/bin/bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

ENV_FILE=${ENV_FILE:-.env}
if [[ ! -f "$ENV_FILE" ]]; then
  ENV_FILE=env/.env.example
fi

set -a
source "$ENV_FILE"
set +a

docker compose -f docker/docker-compose.yml up -d

echo "Waiting for Airflow to initialize..."
until docker compose -f docker/docker-compose.yml exec airflow-webserver airflow db check >/dev/null 2>&1; do
  sleep 5
done

docker compose -f docker/docker-compose.yml exec airflow-webserver airflow users create \
  --username "$AIRFLOW_ADMIN_USER" \
  --firstname Admin --lastname User \
  --role Admin \
  --email "$AIRFLOW_ADMIN_EMAIL" \
  --password "$AIRFLOW_ADMIN_PASSWORD" || true

# Create variables
cat <<JSON | docker compose -f docker/docker-compose.yml exec -T airflow-webserver airflow variables import -
{
  "iceberg_namespace": "${ICEBERG_NAMESPACE}",
  "nessie_branch": "${NESSIE_BRANCH}"
}
JSON

# Seed Mongo with synthetic data
docker compose -f docker/docker-compose.yml exec spark python /opt/spark/scripts/generate_test_data.py --uri "$MONGO_URI"

echo "Bootstrap complete"
