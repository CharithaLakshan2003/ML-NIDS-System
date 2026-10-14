# ============================================================
# Hybrid NIDS - SURICATA PROCESS: Start (Remote Kafka)
# ============================================================

param(
  [string] = "localhost",
  [int] = 29092,
  [string] = "10.23.0.216"
)

 = ":"
      = Split-Path  -Parent
     = "C:\Program Files\Suricata"
     = "C:\Program Files\Elastic\Beats\9.4.4\filebeat"
  = "\filebeat\filebeat.yml"
 = "C:\CICFlowMeter-app\CICFlowMeter-4.0\bin"
    = "C:\CICFlowMeter-app\CICFlowMeter-4.0\bin\data\daily"

Set-Location 

# Update filebeat config for remote Kafka
(Get-Content ) -replace 'hosts:\s*\["[^"]+"\]', "hosts: [""""]" | Set-Content 

Write-Host "
[1/4] Launching Suricata (elevated window)..." -ForegroundColor Cyan
 = "Set-Location ''; & '.\suricata.exe' -i  -c suricata.yaml -vvv"
Start-Process powershell -Verb RunAs -ArgumentList @("-NoExit", "-Command", )
Start-Sleep -Seconds 5

Write-Host "
[2/4] Launching Filebeat (elevated window, writable data path)..." -ForegroundColor Cyan
 = "C:\filebeat-data"
New-Item -ItemType Directory -Force -Path  | Out-Null
 = "Set-Location ''; & '.\filebeat.exe' -e -c '' --path.data ''"
Start-Process powershell -Verb RunAs -ArgumentList @("-NoExit", "-Command", )
Start-Sleep -Seconds 5

Write-Host "
[3/4] Launching CICFlowMeter GUI (elevated window)..." -ForegroundColor Cyan
 = "Set-Location ''; & '.\CICFlowMeter.bat'"
Start-Process powershell -Verb RunAs -ArgumentList @("-NoExit", "-Command", )
Start-Sleep -Seconds 10

Write-Host "
[4/4] Launching CICFlowMeter producer..." -ForegroundColor Cyan
 = "Set-Location ''; if (Test-Path '.\venv\Scripts\Activate.ps1') { & '.\venv\Scripts\Activate.ps1' } ; python producers/cicflowmeter_producer.py --watch-dir '' --bootstrap-servers ''"
Start-Process powershell -ArgumentList @("-NoExit", "-Command", )

Write-Host "
=== Suricata Process launched (Kafka: ) ===" -ForegroundColor Green
