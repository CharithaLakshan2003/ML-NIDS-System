# ============================================================
# Hybrid NIDS - ML ENGINE (EXTERNAL KAFKA): Start
#
# Runs ML engine against an external Kafka broker (e.g. kafka-process
# on another machine). No local Kafka is started. Set KAFKA_BOOTSTRAP
# in ml-engine/.env first: copy .env.example to .env and edit the IP.
#
# Requirements:
# - docker compose
# - .env present with valid KAFKA_BOOTSTRAP (host:port)
# - Network access to the remote Kafka broker
#
# Usage:
#   Windows: powershell -ExecutionPolicy Bypass -File ".\start-ml-engine-external.ps1"
# ============================================================

Set-Location $PSScriptRoot

$envFile = ".env"
if (-not (Test-Path -LiteralPath $envFile)) {
  Write-Host "ERROR: $envFile not found." -ForegroundColor Red
  Write-Host "       Copy .env.example to .env and set KAFKA_BOOTSTRAP to your kafka-process LAN IP (e.g. 192.168.1.100:29092)" -ForegroundColor Red
  exit 1
}

# Load KAFKA_BOOTSTRAP
Get-Content -LiteralPath $envFile | ForEach-Object {
  if ($_ -match '^KAFKA_BOOTSTRAP\s*=\s*(.+?)\s*$') {
    $script:KafkaBootstrap = $matches[1].Trim('"').Trim("'")
  }
}
if (-not $script:KafkaBootstrap) {
  Write-Host "ERROR: KAFKA_BOOTSTRAP not set in .env" -ForegroundColor Red
  exit 1
}

$ComposeFile = "docker-compose.external-kafka.yml"
$Topics     = @("suricata-alerts", "raw-flow-features", "engineered-features", "nids-alerts")

Write-Host "`n[1/6] Preflight: checking reachability to external Kafka: $($script:KafkaBootstrap)" -ForegroundColor Cyan
$preflight = $null
try {
  $preflight = docker run --rm apache/kafka:3.7.0 /opt/kafka/bin/kafka-topics.sh --bootstrap-server $script:KafkaBootstrap --list 2>&1
  Write-Host "      Remote Kafka reachable (topics listed)."
} catch {
  $preflight = $_.Exception.Message
}
if ($preflight) {
  # If there are missing topics, don't fail; just warn.
  Write-Host "      Topics visible? Check above. If required topics are missing, create them on the kafka-process broker." -ForegroundColor Yellow
}

Write-Host "`n[2/6] Building & starting Flink JobManager/TaskManager (external Kafka mode)..." -ForegroundColor Cyan
docker compose -f $ComposeFile up -d --build

Write-Host "`n[3/6] Waiting for JobManager to be ready..." -ForegroundColor Cyan
$deadline = (Get-Date).AddMinutes(3)
do {
  Start-Sleep -Seconds 3
  try {
    $jm = docker exec ml-flink-jobmanager bash -c "curl -s http://localhost:8081/overview" 2>$null
  } catch { $jm = $null }
  if ($jm) { Write-Host "      JobManager is up." }
} while (-not $jm -and (Get-Date) -lt $deadline)

Write-Host "`n[4/6] Fixing Flink checkpoint permissions..." -ForegroundColor Cyan
docker compose -f $ComposeFile exec -u root jobmanager chmod -R 777 /opt/flink/checkpoints
docker compose -f $ComposeFile exec -u root taskmanager chmod -R 777 /opt/flink/checkpoints

Write-Host "`n[5/6] Submitting ML jobs (detached)..." -ForegroundColor Cyan
docker compose -f $ComposeFile exec jobmanager flink run -d -pyFiles /opt/flink/usrlib/flink_jobs/common.py -py /opt/flink/usrlib/flink_jobs/feature_engineering_job.py
docker compose -f $ComposeFile exec jobmanager flink run -d -pyFiles /opt/flink/usrlib/flink_jobs/common.py -py /opt/flink/usrlib/flink_jobs/fusion_job.py

Start-Sleep -Seconds 5
Write-Host "`n[6/6] Running jobs:" -ForegroundColor Green
docker compose -f $ComposeFile exec jobmanager flink list

Write-Host "`n=== ML engine (external Kafka) is up. Connection data: ===" -ForegroundColor Green
Write-Host "  Kafka bootstrap (env)   : $script:KafkaBootstrap"
Write-Host "  Topics (in -> out)       : raw-flow-features, suricata-alerts -> engineered-features, nids-alerts"
Write-Host "  Flink dashboard / REST   : http://localhost:8081"
Write-Host "  Kafka UI (this machine)  : http://localhost:8080"
Write-Host ""
Write-Host "NOTE: Required topics must exist on the external broker (kafka-process)."
Write-Host "      kafka-process can create them via its start script or manually."
Write-Host "Prerequisites: kafka-process's advertised host must be its LAN IP and ports open (29092)."
