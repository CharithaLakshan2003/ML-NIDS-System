#!/usr/bin/env bash
# ============================================================
# Hybrid NIDS - ML ENGINE (EXTERNAL KAFKA): Stop (Linux/Mac)
# ============================================================

set -euo pipefail

cd "$(dirname "$0")"

COMPOSE_FILE="docker-compose.external-kafka.yml"

echo -e "\n[1/3] Cancelling Flink jobs..."
if docker compose -f "$COMPOSE_FILE" ps jobmanager >/dev/null 2>&1; then
  if jobs=$(docker compose -f "$COMPOSE_FILE" exec jobmanager flink list 2>/dev/null | grep nids-); then
    echo "$jobs" | while IFS= read -r line; do
      if [[ -n "$line" ]]; then
        id=$(echo "$line" | awk '{print $1}')
        if [[ $id =~ ^[0-9a-f]{32}$ ]]; then
          echo "      Cancelling job $id"
          docker compose -f "$COMPOSE_FILE" exec jobmanager flink cancel "$id" || true
        fi
      fi
    done
  else
    echo "      No running nids- jobs found."
  fi
else
  echo "      jobmanager not running."
fi

echo -e "\n[2/3] Stopping containers..."
docker compose -f "$COMPOSE_FILE" down

echo -e "\n[3/3] Done. (checkpoints preserved; use 'docker compose -f $COMPOSE_FILE down -v' to wipe)"
