#!/usr/bin/env bash
# ============================================================
# Hybrid NIDS - ML ENGINE (EXTERNAL KAFKA): Start (Linux/Mac)
# ============================================================

set -euo pipefail

cd "$(dirname "$0")"

ENV_FILE=".env"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: $ENV_FILE not found."
  echo "       cp .env.example .env && edit KAFKA_BOOTSTRAP to <kafka-process-LAN-IP>:29092"
  exit 1
fi

KAFKA_BOOTSTRAP=$(grep -E '^KAFKA_BOOTSTRAP\s*=' "$ENV_FILE" | sed -E 's/^KAFKA_BOOTSTRAP\s*=\s*"?([^"]*)"?\s*$/\1/' | tr -d "'")
if [[ -z "$KAFKA_BOOTSTRAP" ]]; then
  echo "ERROR: KAFKA_BOOTSTRAP not set in .env"
  exit 1
fi

COMPOSE_FILE="docker-compose.external-kafka.yml"
echo -e "\n[1/6] Preflight: checking reachability to external Kafka: $KAFKA_BOOTSTRAP"
if docker run --rm apache/kafka:3.7.0 /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --list >/dev/null 2>&1; then
  echo "      Remote Kafka reachable."
else
  echo "      WARNING: Could not list topics on $KAFKA_BOOTSTRAP (broker may not be up/accessible or firewall blocking 29092)."
fi

echo -e "\n[2/6] Building & starting Flink JobManager/TaskManager..."
docker compose -f "$COMPOSE_FILE" up -d --build

echo -e "\n[3/6] Waiting for JobManager..."
deadline=$((SECONDS + 180))
JM_OK=0
while (( SECONDS < deadline )); do
  if docker exec ml-flink-jobmanager curl -sf http://localhost:8081/overview >/dev/null 2>&1; then
    echo "      JobManager is up."
    JM_OK=1
    break
  fi
  sleep 3
done
if (( JM_OK == 0 )); then
  echo "WARNING: JobManager may still be starting; continuing..."
fi

echo -e "\n[4/6] Fixing Flink checkpoint permissions..."
docker compose -f "$COMPOSE_FILE" exec -u root jobmanager chmod -R 777 /opt/flink/checkpoints || true
docker compose -f "$COMPOSE_FILE" exec -u root taskmanager chmod -R 777 /opt/flink/checkpoints || true

echo -e "\n[5/6] Submitting ML jobs (detached)..."
docker compose -f "$COMPOSE_FILE" exec jobmanager flink run -d -pyFiles /opt/flink/usrlib/flink_jobs/common.py -py /opt/flink/usrlib/flink_jobs/feature_engineering_job.py
docker compose -f "$COMPOSE_FILE" exec jobmanager flink run -d -pyFiles /opt/flink/usrlib/flink_jobs/common.py -py /opt/flink/usrlib/flink_jobs/fusion_job.py

sleep 5
echo -e "\n[6/6] Running jobs:"
docker compose -f "$COMPOSE_FILE" exec jobmanager flink list || true

echo -e "\n=== ML engine (external Kafka) is up ==="
echo "  Kafka bootstrap (env)   : $KAFKA_BOOTSTRAP"
echo "  Flink dashboard         : http://localhost:8081"
echo "  Kafka UI                : http://localhost:8080"
echo "  Topics required         : raw-flow-features, suricata-alerts, engineered-features, nids-alerts (on external broker)"
