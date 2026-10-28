#!/usr/bin/env bash
# ============================================================
# Hybrid NIDS - ML ENGINE: Stop (Local mode, Linux/Mac)
# ============================================================

set -euo pipefail

cd "$(dirname "$0")"

echo -e "\n[1/3] Cancelling Flink jobs..."
if docker compose ps jobmanager >/dev/null 2>&1; then
  if jobs=$(docker compose exec jobmanager flink list 2>/dev/null | grep nids-); then
    echo "$jobs" | while IFS= read -r line; do
      if [[ -n "$line" ]]; then
        id=$(echo "$line" | awk '{print $1}')
        if [[ $id =~ ^[0-9a-f]{32}$ ]]; then
          echo "      Cancelling job $id"
          docker compose exec jobmanager flink cancel "$id" || true
        fi
      fi
    done
  else
    echo "      No running nids- jobs found."
  fi
else
  echo "      jobmanager not running."
fi

echo -e "\n[2/3] Stopping Kafka + Flink containers (data preserved)..."
docker compose down

echo -e "\n[3/3] Done."
echo "  Use 'docker compose down -v' if you want to wipe Kafka/Flink data."
