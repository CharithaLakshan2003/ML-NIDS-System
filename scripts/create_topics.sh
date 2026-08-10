#!/usr/bin/env bash
# Creates the 4 topics the pipeline needs. Run after `docker compose up -d`,
# once the kafka container is healthy:
#   ./scripts/create_topics.sh
set -euo pipefail

CONTAINER="nids-kafka"
BOOTSTRAP="localhost:9092"
PARTITIONS="${PARTITIONS:-3}"
REPLICATION=1

topics=(
  "suricata-alerts"
  "raw-flow-features"
  "engineered-features"
  "nids-alerts"
)

for topic in "${topics[@]}"; do
  echo "Creating topic: ${topic}"
  docker exec "${CONTAINER}" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION}"
done

echo
echo "Topics now present:"
docker exec "${CONTAINER}" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --list
