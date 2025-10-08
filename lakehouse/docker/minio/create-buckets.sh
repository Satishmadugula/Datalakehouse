#!/bin/sh
set -euo pipefail

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
for bucket in raw bronze silver gold iceberg-metadata; do
  if ! mc ls local/$bucket >/dev/null 2>&1; then
    mc mb local/$bucket
  fi
  mc anonymous set-json ./policy.json local/$bucket
  mc retention set compliance local/$bucket --default "30d" || true
  mc version enable local/$bucket || true
  mc ilm add local/$bucket --prefix "" --expiry-days 365 || true

done
mc mb local/iceberg-metadata/spark-events >/dev/null 2>&1 || true
