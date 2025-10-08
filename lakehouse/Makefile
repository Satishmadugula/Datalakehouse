.PHONY: up down seed-mongo run-batch run-stream compaction dremio-login

up:
ENV_FILE?=.env
ENV_FILE=$${ENV_FILE} ./scripts/bootstrap_local.sh

down:
./scripts/teardown_local.sh

seed-mongo:
docker compose -f docker/docker-compose.yml exec spark python /opt/spark/scripts/generate_test_data.py

run-batch:
docker compose -f docker/docker-compose.yml exec spark spark-submit /opt/spark/jobs/batch_mongo_to_iceberg.py --namespace $${ICEBERG_NAMESPACE:-payments} --execution-date $${DATE:-`date +%F`}

run-stream:
docker compose -f docker/docker-compose.yml exec spark spark-submit /opt/spark/jobs/stream_kafka_to_iceberg.py --namespace $${ICEBERG_NAMESPACE:-payments}

compaction:
docker compose -f docker/docker-compose.yml exec spark spark-submit /opt/spark/jobs/compaction_rewrite_data_files.py

dremio-login:
@echo "Login to Dremio at http://localhost:9047 with user: $${DREMIO_ADMIN_USER:-dremio} password: $${DREMIO_ADMIN_PASSWORD:-dremio}"
