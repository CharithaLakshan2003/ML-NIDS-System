# ============================================================
# Hybrid NIDS - ML ENGINE: Start
#
# Brings up ONLY the ML engine's infrastructure (Kafka + Flink), creates the
# topics, fixes Flink checkpoint permissions, and submits the two PyFlink ML
# jobs. No Suricata, Filebeat, CICFlowMeter, MinIO, OpenSearch, or backend.
#
# Run from a normal (non-admin) PowerShell window:
#   powershell -ExecutionPolicy Bypass -File ".\ml-engine\start-ml-engine.ps1"
#
# Feed it data with either:
#   * the sample publisher:  python ml-engine\tools\publish_sample_data.py
#   * the full pipeline's producer pointed at localhost:29092
# See ml-engine\README.md.
# ============================================================

Set-Location $PSScriptRoot

$KafkaHost  = "localhost:29092"
$Topics     = @("suricata-alerts", "raw-flow-features", "engineered-features", "nids-alerts")

Write-Host "`n[1/6] Building & starting Kafka + Flink (ML engine infrastructure)..." -ForegroundColor Cyan
docker compose up -d --build

Write-Host "`n[2/6] Waiting for Kafka to become healthy..." -ForegroundColor Cyan
$deadline = (Get-Date).AddMinutes(3)
$health = ""
do {
    Start-Sleep -Seconds 4
    $health = (docker inspect --format "{{.State.Health.Status}}" ml-kafka) 2>$null
    Write-Host "      kafka: $health"
} while ($health -ne "healthy" -and (Get-Date) -lt $deadline)

if ($health -ne "healthy") {
    Write-Host "`nKafka did not report healthy within 3 minutes. Check 'docker compose logs kafka'." -ForegroundColor Red
    exit 1
}

Write-Host "`n[3/6] Creating Kafka topics (idempotent)..." -ForegroundColor Cyan
foreach ($topic in $Topics) {
    docker exec ml-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $topic --partitions 3 --replication-factor 1
}
docker exec ml-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

Write-Host "`n[4/6] Fixing Flink checkpoint permissions..." -ForegroundColor Cyan
docker compose exec -u root jobmanager chmod -R 777 /opt/flink/checkpoints
docker compose exec -u root taskmanager chmod -R 777 /opt/flink/checkpoints

Write-Host "`n[5/6] Submitting ML jobs (detached)..." -ForegroundColor Cyan
Write-Host "      -> Job 1: feature_engineering_job (raw-flow-features -> engineered-features)"
docker compose exec jobmanager flink run -d -pyFiles /opt/flink/usrlib/flink_jobs/common.py -py /opt/flink/usrlib/flink_jobs/feature_engineering_job.py
Write-Host "      -> Job 2: fusion_job (alerts + features -> MLP/AE scoring -> nids-alerts)"
docker compose exec jobmanager flink run -d -pyFiles /opt/flink/usrlib/flink_jobs/common.py -py /opt/flink/usrlib/flink_jobs/fusion_job.py

Start-Sleep -Seconds 5
Write-Host "`n[6/6] Running jobs:" -ForegroundColor Green
docker compose exec jobmanager flink list

Write-Host "`n=== ML engine is up. Connection data: ===" -ForegroundColor Green
Write-Host "  Kafka bootstrap (host)   : $KafkaHost"
Write-Host "  Kafka bootstrap (in-net) : kafka:9092"
Write-Host "  Topics (in -> out)       : raw-flow-features, suricata-alerts -> engineered-features, nids-alerts"
Write-Host "  Flink dashboard / REST   : http://localhost:8081"
Write-Host "  Kafka UI                 : http://localhost:8080"
Write-Host ""
Write-Host "Publish sample data (separate window):" -ForegroundColor Yellow
Write-Host "  .\venv\Scripts\Activate.ps1"
Write-Host "  python ml-engine\tools\publish_sample_data.py"
Write-Host ""
Write-Host "Watch the ML output (separate window):" -ForegroundColor Yellow
Write-Host "  python ml-engine\tools\consume_topic.py --topic nids-alerts"
