# ============================================================
# Hybrid NIDS - Start Pipeline
# Run this from a normal (non-admin) PowerShell window, in the
# project root: D:\3rd year project\NIDS_with_kafka_and_flink
#
#   powershell -ExecutionPolicy Bypass -File ".\start-pipeline.ps1"
#
# NOTE: Suricata, Filebeat, and CICFlowMeter all launch in ELEVATED windows
# (they need admin rights to capture packets / write to Program Files).
# Windows will show a UAC prompt for each one - click "Yes" on all 3,
# ideally quickly, since later steps assume they're already running.
# ============================================================

# ---- EDIT THESE PATHS TO MATCH YOUR MACHINE -----------------
$ProjectDir      = "D:\3rd year project\NIDS_with_kafka_and_flink"
$SuricataDir     = "C:\Program Files\Suricata"
# $SuricataIface   = "10.21.95.184"                   
$SuricataIface   = "192.168.8.100" 
# $SuricataIface   = "10.23.0.216"   # your Wi-Fi adapter's IP address
$FilebeatDir     = "C:\Program Files\Elastic\Beats\9.4.4\filebeat"
$FilebeatConfig  = "$ProjectDir\filebeat\filebeat.yml"
$CICFlowMeterDir = "C:\CICFlowMeter-app\CICFlowMeter-4.0\bin"

$CICOutputDir    = "C:\CICFlowMeter-app\CICFlowMeter-4.0\bin\data\daily"
# ---------------------------------------------------------------

Set-Location $ProjectDir

Write-Host "`n[1/8] Starting Docker (Kafka + Flink cluster + MinIO + OpenSearch + backend)..." -ForegroundColor Cyan
docker compose up -d --build
Start-Sleep -Seconds 20
docker compose ps

Write-Host "`n[2/8] Creating Kafka topics (if they don't already exist)..." -ForegroundColor Cyan
$topics = @("suricata-alerts", "raw-flow-features", "engineered-features", "nids-alerts")
foreach ($topic in $topics) {
    docker exec nids-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $topic --partitions 3 --replication-factor 1
}
docker exec nids-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

Write-Host "`n[3/8] Fixing Flink checkpoint permissions..." -ForegroundColor Cyan
docker compose exec -u root jobmanager chmod -R 777 /opt/flink/checkpoints
docker compose exec -u root taskmanager chmod -R 777 /opt/flink/checkpoints

Write-Host "`n[4/8] Launching Suricata (elevated window)..." -ForegroundColor Cyan
$suricataScript = "Set-Location '$SuricataDir'; & '.\suricata.exe' -i $SuricataIface -c suricata.yaml -vvv"
Start-Process powershell -Verb RunAs -ArgumentList @("-NoExit", "-Command", $suricataScript)
Start-Sleep -Seconds 5

Write-Host "`n[5/8] Launching Filebeat (elevated window, writable data path)..." -ForegroundColor Cyan
$FilebeatDataDir = "C:\filebeat-data"
New-Item -ItemType Directory -Force -Path $FilebeatDataDir | Out-Null
$filebeatScript = "Set-Location '$FilebeatDir'; & '.\filebeat.exe' -e -c '$FilebeatConfig' --path.data '$FilebeatDataDir'"
Start-Process powershell -Verb RunAs -ArgumentList @("-NoExit", "-Command", $filebeatScript)
Start-Sleep -Seconds 5

Write-Host "`n[6/8] Launching CICFlowMeter GUI (elevated window)..." -ForegroundColor Cyan
Write-Host "      -> Select your Wi-Fi interface and click Start manually in the GUI." -ForegroundColor Yellow
$cicScript = "Set-Location '$CICFlowMeterDir'; & '.\CICFlowMeter.bat'"
Start-Process powershell -Verb RunAs -ArgumentList @("-NoExit", "-Command", $cicScript)
Start-Sleep -Seconds 3

Write-Host "`n[7/8] Launching the producer (activating venv + ensuring deps first)..." -ForegroundColor Cyan
Write-Host "      -> Waiting a bit for CICFlowMeter to create its first CSV file." -ForegroundColor Yellow
Start-Sleep -Seconds 10
$producerScript = "Set-Location '$ProjectDir'; " +
    "if (Test-Path '.\venv\Scripts\Activate.ps1') { & '.\venv\Scripts\Activate.ps1' } " +
    "else { pip install -r requirements.txt --quiet } ; " +
    "python producers/cicflowmeter_producer.py --watch-dir '$CICOutputDir' --bootstrap-servers localhost:29092"
Start-Process powershell -ArgumentList @("-NoExit", "-Command", $producerScript)
Start-Sleep -Seconds 5

Write-Host "`n[8/8] Submitting Flink jobs (detached mode, so this doesn't block)..." -ForegroundColor Cyan
docker compose exec jobmanager flink run -d -pyFiles /opt/flink/usrlib/flink_jobs/common.py -py /opt/flink/usrlib/flink_jobs/feature_engineering_job.py
docker compose exec jobmanager flink run -d -pyFiles /opt/flink/usrlib/flink_jobs/common.py -py /opt/flink/usrlib/flink_jobs/fusion_job.py

Start-Sleep -Seconds 5
Write-Host "`n=== Process check (each should show a running process) ===" -ForegroundColor Green
$processChecks = [ordered]@{
    "Suricata"            = "suricata"
    "Filebeat"            = "filebeat"
    "Java (CICFlowMeter)" = "java"
    "Python (producer)"   = "python"
}
foreach ($label in $processChecks.Keys) {
    $procs = Get-Process $processChecks[$label] -ErrorAction SilentlyContinue
    if ($procs) {
        $pids = ($procs | ForEach-Object { $_.Id }) -join ", "
        Write-Host ("  {0,-20}: running (PID {1})" -f $label, $pids)
    } else {
        Write-Host ("  {0,-20}: NOT RUNNING" -f $label) -ForegroundColor Red
    }
}

Write-Host "`n=== All components launched. Check: ===" -ForegroundColor Green
Write-Host "  Kafka UI:              http://localhost:8080"
Write-Host "  Flink Dashboard:       http://localhost:8081"
Write-Host "  MinIO Console:         http://localhost:9501  (minioadmin / minioadmin)"
Write-Host "  OpenSearch:            http://localhost:9400"
Write-Host "  OpenSearch Dashboards: http://localhost:5601"
Write-Host "  Backend API docs:      http://localhost:8000/docs"
docker compose exec jobmanager flink list

Write-Host "`n=== Layer 5 container health ===" -ForegroundColor Green
docker compose ps minio opensearch backend datalake-writer alert-indexer