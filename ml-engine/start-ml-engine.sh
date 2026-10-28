#!/usr/bin/env bash
# ============================================================
# Hybrid NIDS - ML ENGINE: Start (Local mode, Linux/Mac)
# ============================================================

set -euo pipefail

cd "$(dirname "$0")"

KAFKA_HOST="localhost:29092"
TOPICS=("suricata-alerts" "raw-flow-features" "engineered-features" "nids-alerts")

echo -e "\n[1/6] Building & starting Kafka + Flink (ML engine infrastructure)..."
docker compose up -d --build

echo -e "\n[2/6] Waiting for Kafka to become healthy..."
deadline=$((SECONDS + 180))
HEALTH=""
while (( SECONDS < deadline )); do
  HEALTH=$(docker inspect --format '{{.State.Health.Status}}' ml-kafka 2>/dev/null || true)
  echo "      kafka: $HEALTH"
  [[ "$HEALTH" == "healthy" ]] && break
  sleep 4
done
if [[ "$HEALTH" != "healthy" ]]; then
  echo -e "\nKafka did not report healthy within 3 minutes. Check 'docker compose logs kafka'."
  exit 1
fi

echo -e "\n[3/6] Creating Kafka topics (idempotent)..."
for t in "${TOPICS[@]}"; do
  docker exec ml-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "$t" --partitions 3 --replication-factor 1
done
docker exec ml-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

echo -e "\n[4/6] Fixing Flink checkpoint permissions..."
docker compose exec -u root jobmanager chmod -R 777 /opt/flink/checkpoints || true
docker compose exec -u root taskmanager chmod -R 777 /opt/flink/checkpoints || true

echo -e "\n[5/6] Submitting ML jobs (detached)..."
docker compose exec jobmanager flink run -d -pyFiles /opt/flink/usrlib/flink_jobs/common.py -py /opt/flink/usrlib/flink_jobs/feature_engineering_job.py
docker compose exec jobmanager flink run -d -pyFiles /opt/flink/usrlib/flink_jobs/common.py -py /opt/flink/usrlib/flink_jobs/fusion_job.py

sleep 5
echo -e "\n[6/6] Running jobs:"
docker compose exec jobmanager flink list || true

echo -e "\n=== ML engine is up. Connection data: ==="
echo "  Kafka bootstrap (host)   : $KAFKA_HOST"
echo "  Kafka bootstrap (in-net) : kafka:9092"
echo "  Topics                  : raw-flow-features, suricata-alerts, engineered-features, nids-alerts"
echo "  Flink dashboard         : http://localhost:8081"
echo "  Kafka UI                : http://localhost:8080"
echo -e "\nPublish sample data: python ml-engine/tools/publish_sample_data.py"
echo "Watch output:          python ml-engine/tools/consume_topic.py --topic nids-alerts"
