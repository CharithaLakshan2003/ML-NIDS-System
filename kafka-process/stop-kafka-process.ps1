# ============================================================
# Hybrid NIDS - KAFKA PROCESS: Stop
# ============================================================

Set-Location 
Write-Host "
[1/2] Stopping Kafka Process containers..." -ForegroundColor Cyan
docker compose down
Write-Host "
[2/2] Done." -ForegroundColor Green
