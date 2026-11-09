#!/usr/bin/env bash
# Creates the Kafka topics the pipeline uses. Safe to run more than once.
set -euo pipefail
BOOTSTRAP=${BOOTSTRAP:-kafka:9092}
for topic in patient-events patient-events-clean department-stats patient-events-dlq; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions 3 --replication-factor 1
done
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
