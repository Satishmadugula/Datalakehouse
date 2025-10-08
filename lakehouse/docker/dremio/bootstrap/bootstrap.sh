#!/bin/bash
set -euo pipefail
/opt/dremio/bin/dremio start &
PID=$!

until curl -sf http://localhost:9047/apiv2/login >/dev/null; do
  sleep 5
done

TOKEN=$(curl -s -X POST http://localhost:9047/apiv2/login -H 'Content-Type: application/json' -d '{"userName":"'${DREMIO_USERNAME:-dremio}'","password":"'${DREMIO_PASSWORD:-dremio}'"}' | jq -r '.token')
AUTH="_dremio$TOKEN"

for file in /opt/dremio/bootstrap/*.json; do
  name=$(basename "$file")
  case "$name" in
    add_minio_source.json|add_iceberg_nessie_source.json)
      curl -sf -X POST http://localhost:9047/apiv2/source \
        -H "authorization: $AUTH" \
        -H 'Content-Type: application/json' \
        --data-binary @"$file" || true
      ;;
    create_reflections.json)
      curl -sf -X POST http://localhost:9047/api/v3/reflection?accelerator=true \
        -H "authorization: $AUTH" \
        -H 'Content-Type: application/json' \
        --data-binary @"$file" || true
      ;;
  esac
done

wait $PID
