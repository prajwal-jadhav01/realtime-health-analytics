#!/usr/bin/env bash
# Registers the Pinot schemas and REALTIME tables. The controller has Basic Auth on,
# so every admin call must carry credentials; without them it returns 401.
# Safe to re-run: tables that already exist are skipped.
set -euo pipefail
CONTROLLER=${CONTROLLER:-pinot-controller}
for name in patient_events department_stats; do
  if curl -sf -u "$PINOT_USER:$PINOT_PASSWORD" "http://$CONTROLLER:9000/tables/$name" | grep -q "$name"; then
    echo "Table $name already exists, skipping"
    continue
  fi
  echo "Creating table $name"
  /opt/pinot/bin/pinot-admin.sh AddTable \
    -schemaFile "/pinot/schemas/${name}.schema.json" \
    -tableConfigFile "/pinot/tables/${name}.table.json" \
    -controllerHost "$CONTROLLER" -controllerPort 9000 \
    -user "$PINOT_USER" -password "$PINOT_PASSWORD" \
    -exec
done
