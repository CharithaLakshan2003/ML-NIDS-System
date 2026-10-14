# ============================================================
# Hybrid NIDS - KAFKA PROCESS: Start (Remote Access)
# Updates Kafka advertised listener to host LAN IP
# ============================================================

param(
  [string] = "localhost"
)

Set-Location 

# Update docker-compose with host IP
 = Get-Content "docker-compose.yml" -Raw
 =  -replace 'PLAINTEXT_HOST://localhost:29092', "PLAINTEXT_HOST://:29092"
Set-Content "docker-compose.yml" 

Write-Host "Updated Kafka advertised listener to PLAINTEXT_HOST://:29092" -ForegroundColor Yellow

# Run normal start
.\start-kafka-process.ps1
