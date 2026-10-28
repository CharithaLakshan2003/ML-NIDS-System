# ============================================================
# Hybrid NIDS - ML ENGINE (EXTERNAL KAFKA): Stop
#
# Cancels the two Flink ML jobs, then tears down JobManager/TaskManager.
#
# Usage:
#   Windows: powershell -ExecutionPolicy Bypass -File ".\stop-ml-engine-external.ps1"
# ============================================================

Set-Location $PSScriptRoot

$ComposeFile = "docker-compose.external-kafka.yml"

Write-Host "`n[1/3] Cancelling Flink jobs..." -ForegroundColor Cyan
try {
  $jobs = docker compose -f $ComposeFile exec jobmanager flink list 2>$null
  if ($jobs) {
    $jobIds = $jobs | Select-String -Pattern '([0-9a-f]{32})\s*:\s*nids-' | ForEach-Object {
      ($_ -split '\s*:\s*')[0].Trim()
    }
    if (-not $jobIds) {
      Write-Host "      No running nids- jobs found." -ForegroundColor Yellow
    }
    foreach ($id in $jobIds) {
      Write-Host "      Cancelling job $id"
      docker compose -f $ComposeFile exec jobmanager flink cancel $id
    }
  }
} catch {
  Write-Host "      Could not list/cancel jobs (jobmanager may be down)." -ForegroundColor Yellow
}

Write-Host "`n[2/3] Stopping Flink containers (data preserved)..." -ForegroundColor Cyan
docker compose -f $ComposeFile down

Write-Host "`n[3/3] Done." -ForegroundColor Green
Write-Host "  Checkpoints preserved in named volume. To wipe: docker compose -f $ComposeFile down -v"
