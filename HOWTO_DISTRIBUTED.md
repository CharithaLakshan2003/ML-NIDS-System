## 2) Laptop B - ml-engine (Flink jobs, NO local Kafka)

Use the external-Kafka mode for ml-engine (recommended). 

On Laptop B (ml-engine):

1. Copy .env.example to .env and set Kafka bootstrap to Laptop A's LAN IP:
   `powershell
   cd ml-engine
   Copy-Item .env.example .env
   # edit .env: KAFKA_BOOTSTRAP=192.168.1.100:29092
   `

2. Start in external mode:
   `powershell
   powershell -ExecutionPolicy Bypass -File ".\start-ml-engine-external.ps1"
   `
   (or ./start-ml-engine-external.sh on Linux/Mac)

Prerequisites:
- Laptop A (kafka-process): set KAFKA_ADVERTISED_HOST=192.168.1.100 in kafka-process/.env so remote clients connect to Laptop A's LAN IP (default keeps localhost for single-machine). Also ensure port 29092 is reachable.
- Start kafka-process on Laptop A before starting ml-engine.
