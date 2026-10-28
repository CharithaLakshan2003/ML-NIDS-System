# Hybrid NIDS — ML Engine (standalone)

This folder is the machine-learning part of the Hybrid NIDS, extracted so it can be developed and run on its own — without Suricata, Filebeat, CICFlowMeter, MinIO, OpenSearch, or the FastAPI backend.

It contains exactly the two PyFlink streaming jobs that make up the ML engine, their trained model artifacts, and a minimal Kafka + Flink harness to run them.

```
raw-flow-features ──┐
                    ├──► feature_engineering_job ──► engineered-features ──┐
suricata-alerts ────┼──► (impute/order via training medians)               │
                    │                                                       ▼
                    └──────────────────────────────────► fusion_job ──► nids-alerts
                                     (MLP + Autoencoder scoring, severity tiering)
```

## 1. What's in this folder

`
ml-engine/
├── docker-compose.yml            Kafka + Kafka-UI + Flink JobManager/TaskManager (ONLY, local mode)
├── docker-compose.external-kafka.yml  Flink only, connects to external Kafka (recommended for separate machines)
├── .env.example                   Example for external Kafka bootstrap
├── Dockerfile.flink              Flink 1.18 image: PyFlink + TensorFlow + scikit-learn + Kafka connector jar
├── requirements.txt              Deps installed INSIDE the Flink image
├── requirements-tools.txt        Host-side deps for tools/ (kafka-python)
├── start-ml-engine.ps1           Starts local infra + submits both ML jobs (Windows)
├── stop-ml-engine.ps1            Cancels jobs + tears down local infra (Windows)
├── start-ml-engine.sh            Same as above (Linux/Mac)
├── stop-ml-engine.sh             Same as above (Linux/Mac)
├── start-ml-engine-external.ps1  Connect to external Kafka + submit jobs (Windows)
├── stop-ml-engine-external.ps1   Stop external-mode stack (Windows)
├── start-ml-engine-external.sh   Same as above (Linux/Mac)
├── stop-ml-engine-external.sh    Same as above (Linux/Mac)
│
├── flink_jobs/                   ML job code
│   ├── common.py                   Shared topic names, Kafka bootstrap, flow_key() join key
│   ├── feature_engineering_job.py  Job 1: order + impute raw features using training medians
│   └── fusion_job.py               Job 2: keyed TTL join + MLP/AE scoring + severity tiers
│
├── models/                       8 trained artifacts (see §7)
│
└── tools/                        Standalone test helpers (host-side, use the venv)
    ├── publish_sample_data.py      Publish synthetic flows + Suricata alerts to Kafka
    └── consume_topic.py            Print messages from any ML-engine topic
`

> **Port note (local mode):** docker-compose.yml uses host ports 29092, 8080, 8081. This means the full pipeline and the local ML engine cannot run at the same time on one machine — stop one before starting the other. External mode does not require port 29092 on the ML engine machine (only 8080, 8081).

## 2. Connection interface (how it connects)

The ML engine is **Kafka-only**: it reads/writes the 4 topics below. In distributed deployments, it connects to the kafka-process broker over your LAN.

| Direction | Kafka topic | Who produces | Who consumes | Message shape |
|---|---|---|---|---|
| **IN** | aw-flow-features | suricata-process (CICFlowMeter producer) | ml-engine feature_engineering_job | CICFlowMeter-style JSON: training-named columns + src_ip, dst_ip, dst_port, 	imestamp |
| **IN** | suricata-alerts | suricata-process (Filebeat) | ml-engine fusion_job | eve.json alert JSON: src_ip, dest_ip, dest_port, 	imestamp, lert.{signature,category,severity,signature_id} |
| **OUT** | engineered-features | ml-engine feature_engineering_job | kafka-process datalake-writer + ml-engine fusion_job | ordered + median-imputed raw vector (still unscaled) + low_key |
| **OUT** | 
ids-alerts | ml-engine fusion_job | kafka-process alert-indexer + backend WS | fused alert: severity_tier, easons, mlp{}, utoencoder{}, suricata_alert |

**Join key** (common.flow_key, shared by both jobs and the producers): "<src_ip>-<dst_ip>-<dst_port>" — e.g. 10.0.0.15-10.0.0.8-443

Example messages (unchanged from original; see full README for details).

**Connection endpoints (single source of truth):**

| Context | Kafka bootstrap | Notes |
|---|---|---|
| From host, same machine (local mode) | localhost:29092 | Used by 	ools/publish_sample_data.py, 	ools/consume_topic.py when pointing to local ML-engine broker |
| From inside Docker (local mode) | kafka:9092 | Internal network (ml-kafka service) |
| External mode (remote ML engine) | <kafka-process LAN IP>:29092 | Set in ml-engine/.env as KAFKA_BOOTSTRAP |

**Environment variables (flink_jobs/common.py):**

| Var | Default | Purpose |
|---|---|---|
| KAFKA_BOOTSTRAP | kafka:9092 | Kafka broker the jobs read/write |
| MODELS_DIR | /opt/flink/usrlib/models | Where the 8 model artifacts live |

## 3. Start / stop

### A. Local mode (same machine, includes local Kafka)

**Windows:**
`powershell
# Start
powershell -ExecutionPolicy Bypass -File ".\ml-engine\start-ml-engine.ps1"
# Stop
powershell -ExecutionPolicy Bypass -File ".\ml-engine\stop-ml-engine.ps1"
`

**Linux/Mac:**
`ash
./ml-engine/start-ml-engine.sh
./ml-engine/stop-ml-engine.sh
`

### B. External mode (connect to remote kafka-process)

1. On the ML engine machine, copy env and set broker IP:
`ash
cd ml-engine
cp .env.example .env
# edit .env: KAFKA_BOOTSTRAP=192.168.1.100:29092
`

2. On the kafka-process machine, ensure it's reachable remotely:
- Set KAFKA_ADVERTISED_HOST in kafka-process/.env to that machine's LAN IP (see kafka-process/.env.example), or edit its docker-compose.yml advertised listener.
- Start kafka-process (creates/hosts the topics via its compose/scripts).
- Open port 29092 on kafka-process machine's firewall (LAN only).

3. Start ML engine on its own machine:
`powershell
powershell -ExecutionPolicy Bypass -File ".\ml-engine\start-ml-engine-external.ps1"
`
or
`ash
./ml-engine/start-ml-engine-external.sh
`

Stop with the matching stop-ml-engine-external.*.

**Order matters:** start kafka-process first (or ensure broker+topics exist), then ml-engine, then suricata-process. On shutdown, cancel/stop in reverse order.
