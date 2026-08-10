# ============================================================
# Hybrid NIDS - Stop Pipeline
# Run this from a normal (non-admin) PowerShell window, in the
# project root: D:\3rd year project\NIDS_with_kafka_and_flink
# ============================================================

$ProjectDir = "D:\3rd year project\NIDS_with_kafka_and_flink"
Set-Location $ProjectDir

Write-Host "`n[1/4] Cancelling Flink jobs..." -ForegroundColor Cyan
$jobs = docker compose exec jobmanager flink list 2>$null
$jobIds = $jobs | Select-String -Pattern '([0-9a-f]{32})\s*:\s*nids-' | ForEach-Object {
    ($_ -split '\s*:\s*')[0].Trim()
}
foreach ($id in $jobIds) {
    Write-Host "  Cancelling job $id"
    docker compose exec jobmanager flink cancel $id
}

Write-Host "`n[2/4] Stopping the producer, Filebeat, Suricata, CICFlowMeter..." -ForegroundColor Cyan
Write-Host "      -> These run in their own windows and can't be closed remotely." -ForegroundColor Yellow
Write-Host "      -> Go to each window and press Ctrl+C (or click Stop in the CICFlowMeter GUI, then close it)." -ForegroundColor Yellow

# Best-effort: kill known process names if you'd rather not Ctrl+C each one manually.
# Uncomment the lines below if you want this script to force-close them instead.
# Stop-Process -Name "suricata"       -ErrorAction SilentlyContinue
# Stop-Process -Name "filebeat"       -ErrorAction SilentlyContinue
# Stop-Process -Name "java"           -ErrorAction SilentlyContinue   # CICFlowMeter runs on the JVM
# Get-Process python* -ErrorAction SilentlyContinue |
#     Where-Object { $_.Path -and (Get-CimInstance Win32_Process -Filter "ProcessId=$($_.Id)").CommandLine -match "cicflowmeter_producer" } |
#     Stop-Process -Force

Read-Host "`nPress Enter once you've closed Suricata / Filebeat / CICFlowMeter / producer windows"

Write-Host "`n[3/4] Stopping Docker containers (data is preserved)..." -ForegroundColor Cyan
docker compose down

Write-Host "`n[4/4] Done." -ForegroundColor Green
Write-Host "  Use 'docker compose down -v' instead next time if you also want to wipe Kafka/Flink data."