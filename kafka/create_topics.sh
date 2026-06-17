#!/usr/bin/env bash
# Creates Kafka topics for the IoT pipeline.
# Idempotent: --if-not-exists makes it safe to run multiple times.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"

echo "[kafka-init] bootstrap: ${BOOTSTRAP}"

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" \
  --create --if-not-exists \
  --topic iot_events \
  --partitions 3 \
  --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" \
  --create --if-not-exists \
  --topic iot_aggregated \
  --partitions 3 \
  --replication-factor 1

echo "[kafka-init] topics created:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --list
