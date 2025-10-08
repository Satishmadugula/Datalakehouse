#!/bin/bash
set -euo pipefail

/opt/nifi/nifi-current/bin/nifi.sh start

while ! curl -s "http://localhost:9090/nifi-api/system-diagnostics" >/dev/null; do
  echo "Waiting for NiFi..."
  sleep 5
done

echo "NiFi is up. Importing templates."
for template in /opt/nifi/nifi-current/conf/templates/*.xml; do
  curl -sf -X POST \
    -H 'Content-Type: application/xml' \
    --data-binary @"${template}" \
    "http://localhost:9090/nifi-api/workflow/templates" || true

done

tail -f /opt/nifi/nifi-current/logs/nifi-app.log
