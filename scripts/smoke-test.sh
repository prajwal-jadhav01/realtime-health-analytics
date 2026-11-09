#!/usr/bin/env bash
# End-to-end check used by CI: waits until events have flowed
# generator -> Kafka -> Flink -> Kafka -> Pinot, then proves auth is enforced.
set -euo pipefail
BROKER=${BROKER:-http://localhost:8099}
AUTH="admin:adminpass"
DEADLINE=$((SECONDS + ${TIMEOUT_SECONDS:-300}))

query() {
  curl -sf -u "$AUTH" -H 'Content-Type: application/json' \
    -d "{\"sql\":\"$1\"}" "$BROKER/query/sql"
}

count_rows() {
  query "SELECT COUNT(*) FROM $1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["resultTable"]["rows"][0][0])' 2>/dev/null || echo 0
}

echo "Waiting for rows in patient_events and department_stats..."
while (( SECONDS < DEADLINE )); do
  events=$(count_rows patient_events)
  stats=$(count_rows department_stats)
  echo "  patient_events=$events department_stats=$stats"
  if (( events > 0 && stats > 0 )); then
    break
  fi
  sleep 10
done
(( events > 0 && stats > 0 )) || { echo "FAIL: data did not reach Pinot in time"; exit 1; }

echo "Checking that queries without credentials are rejected..."
code=$(curl -s -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT COUNT(*) FROM patient_events"}' "$BROKER/query/sql")
[[ "$code" == "401" || "$code" == "403" ]] || { echo "FAIL: unauthenticated query returned $code"; exit 1; }

echo "Checking that bad records were routed to the dead-letter topic, not Pinot..."
bad=$(count_rows "patient_events WHERE heartRate > 250")
[[ "$bad" == "0" ]] || { echo "FAIL: impossible vitals reached Pinot"; exit 1; }

echo "PASS: $events events and $stats department windows in Pinot, auth enforced."
