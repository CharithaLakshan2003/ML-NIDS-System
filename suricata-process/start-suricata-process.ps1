# ============================================================
# Hybrid NIDS - SURICATA PROCESS: Start
#
# Launches Suricata, Filebeat, CICFlowMeter and the CICFlowMeter
# producer to ingest data into Kafka (expects Kafka at localhost:29092).
# ============================================================

$ProjectDir      = Split-Path $PSScriptRoot -Parent
$SuricataDir     = "C:\Program Files\Suricata"
$SuricataIface   = "10.23.0.216"
$FilebeatDir     = "C:\Program Files\Elastic\Beats\9.4.4\filebeat"
$FilebeatConfig  = "$PSScriptRoot\filebeat\filebeat.yml"
$CICFlowMeterDir = "C:\CICFlowMeter-app\CICFlowMeter-4.0\bin"
$CICOutputDir    = "C:\CICFlowMeter-app\CICFlowMeter-4.0\bin\data\daily"

Set-Location $PSScriptRoot

Write-Host "`n[1/4] Launching Suricata (elevated window)..." -ForegroundColor Cyan
$suricataScript = "Set-Location '$SuricataDir'; & '.\suricata.exe' -i $SuricataIface -c suricata.yaml -vvv"
Start-Process powershell -Verb RunAs -ArgumentList @("-NoExit", "-Command", $suricataScript)
Start-Sleep -Seconds 5

Write-Host "`n[2/4] Launching Filebeat (elevated window, writable data path)..." -ForegroundColor Cyan
$FilebeatDataDir = "C:\filebeat-data"
New-Item -ItemType Directory -Force -Path $FilebeatDataDir | Out-Null
$filebeatScript = "Set-Location '$FilebeatDir'; & '.\filebeat.exe' -e -c '$FilebeatConfig' --path.data '$FilebeatDataDir'"
Start-Process powershell -Verb RunAs -ArgumentList @("-NoExit", "-Command", $filebeatScript)
Start-Sleep -Seconds 5

Write-Host "`n[3/4] Launching CICFlowMeter GUI (elevated window)..." -ForegroundColor Cyan
$cicScript = "Set-Location '$CICFlowMeterDir'; & '.\CICFlowMeter.bat'"
Start-Process powershell -Verb RunAs -ArgumentList @("-NoExit", "-Command", $cicScript)
Start-Sleep -Seconds 10

Write-Host "`n[4/4] Launching CICFlowMeter producer..." -ForegroundColor Cyan
$producerScript = "Set-Location '$ProjectDir'; if (Test-Path '.\venv\Scripts\Activate.ps1') { & '.\venv\Scripts\Activate.ps1' } else { Write-Host 'No venv found' } ; python producers/cicflowmeter_producer.py --watch-dir '$CICOutputDir' --bootstrap-servers localhost:29092"
Start-Process powershell -ArgumentList @("-NoExit", "-Command", $producerScript)

Write-Host "`n=== Suricata Process launched ===" -ForegroundColor Green
