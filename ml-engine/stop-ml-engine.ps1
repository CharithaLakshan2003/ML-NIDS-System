# ============================================================
# Hybrid NIDS - ML ENGINE: Stop
#
# Cancels the two Flink ML jobs, then tears down the Kafka + Flink
# containers. Data in the flink-checkpoints volume is preserved.
#
#   powershell -ExecutionPolicy Bypass -File ".\ml-engine\stop-ml-engine.ps1"
# ============================================================

Set-Location $PSScriptRoot

Write-Host "`n[1/3] Cancelling Flink jobs..." -ForegroundColor Cyan
$jobs = docker compose exec jobmanager flink list 2>$null
$jobIds = $jobs | Select-String -Pattern '([0-9a-f]{32})\s*:\s*nids-' | ForEach-Object {
    ($_ -split '\s*:\s*')[0].Trim()
}
if (-not $jobIds) {
    Write-Host "      No running nids- jobs found." -ForegroundColor Yellow
}
foreach ($id in $jobIds) {
    Write-Host "      Cancelling job $id"
    docker compose exec jobmanager flink cancel $id
}

Write-Host "`n[2/3] Stopping Kafka + Flink containers (data preserved)..." -ForegroundColor Cyan
docker compose down

Write-Host "`n[3/3] Done." -ForegroundColor Green
Write-Host "  Use 'docker compose down -v' from ml-engine\ if you also want to wipe Kafka/Flink data."
