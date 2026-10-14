# ============================================================
# Hybrid NIDS - KAFKA PROCESS: Start
#
# Brings up Kafka, Kafka UI, MinIO, OpenSearch, and the
# downstream consumers/services + backend that read from Kafka.
# ============================================================

Set-Location $PSScriptRoot

Write-Host "`n[1/4] Starting Kafka + storage + services..." -ForegroundColor Cyan
docker compose up -d --build

Write-Host "`n[2/4] Waiting for Kafka to become healthy..." -ForegroundColor Cyan
$deadline = (Get-Date).AddMinutes(3)
$health = ""
do {
    Start-Sleep -Seconds 4
    $health = (docker inspect --format "{{.State.Health.Status}}" nids-kafka) 2>$null
    Write-Host "      kafka: $health"
} while ($health -ne "healthy" -and (Get-Date) -lt $deadline)

if ($health -ne "healthy") {
    Write-Host "`nKafka did not report healthy within 3 minutes." -ForegroundColor Red
    exit 1
}

Write-Host "`n[3/4] Creating Kafka topics (idempotent)..." -ForegroundColor Cyan
$Topics = @("suricata-alerts", "raw-flow-features", "engineered-features", "nids-alerts")
foreach ($topic in $Topics) {
    docker exec nids-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $topic --partitions 3 --replication-factor 1
}
docker exec nids-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

Write-Host "`n[4/4] Containers status..." -ForegroundColor Green
docker compose ps

Write-Host "`n=== Kafka Process is up ===" -ForegroundColor Green
Write-Host "  Kafka UI:              http://localhost:8080"
Write-Host "  MinIO Console:         http://localhost:9501  (minioadmin / minioadmin)"
Write-Host "  OpenSearch:            http://localhost:9400"
Write-Host "  OpenSearch Dashboards: http://localhost:5601"
Write-Host "  Backend API docs:      http://localhost:8000/docs"
