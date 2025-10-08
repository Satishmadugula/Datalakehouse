#!/bin/bash
set -euo pipefail
if [[ "${ENABLE_KAFKA:-true}" != "true" ]]; then
  echo "Kafka disabled. Sleeping..."
  tail -f /dev/null
  exit 0
fi
exec /opt/bitnami/scripts/kafka/run.sh
