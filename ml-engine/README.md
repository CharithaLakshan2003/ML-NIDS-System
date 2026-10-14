# Hybrid NIDS — ML Engine (standalone)

This folder is the **machine-learning part of the Hybrid NIDS, extracted so it
can be developed and run on its own** — without Suricata, Filebeat,
CICFlowMeter, MinIO, OpenSearch, or the FastAPI backend.

It contains exactly the two PyFlink streaming jobs that make up the ML engine,
their trained model artifacts, and a minimal Kafka + Flink harness to run them.

```
raw-flow-features ──┐
                    ├──► feature_engineering_job ──► engineered-features ──┐
suricata-alerts ────┼──► (impute/order via training medians)               │
                    │                                                       ▼
                    └──────────────────────────────────► fusion_job ──► nids-alerts
                                     (MLP + Autoencoder scoring, severity tiering)
```

---

## 1. What's in this folder

```
ml-engine/
├── docker-compose.yml            Kafka + Kafka-UI + Flink JobManager/TaskManager (ONLY)
├── Dockerfile.flink              Flink 1.18 image: PyFlink + TensorFlow + scikit-learn + Kafka connector jar
├── requirements.txt              Deps installed INSIDE the Flink image
├── requirements-tools.txt        Host-side deps for tools/ (kafka-python)
├── start-ml-engine.ps1           ⭐ Starts infra, creates topics, submits both ML jobs
├── stop-ml-engine.ps1            ⭐ Cancels jobs and tears down infra
│
├── flink_jobs/                   (copies of the repo's ML job code)
│   ├── common.py                   Shared topic names, Kafka bootstrap, flow_key() join key
│   ├── feature_engineering_job.py  Job 1: order + impute raw features using training medians
│   └── fusion_job.py               Job 2: keyed TTL join + MLP/AE scoring + severity tiers
│
├── models/                       8 trained artifacts (see §7)
│
└── tools/                        Standalone test helpers (host-side, use the venv)
    ├── publish_sample_data.py      Publish synthetic flows + Suricata alerts to Kafka
    └── consume_topic.py            Print messages from any ML-engine topic
```

> **Port note:** the ML engine deliberately uses the *same* host ports as the
> full pipeline (`29092`, `8080`, `8081`) so it speaks the exact same Kafka
> contract. This means the **full pipeline and this ML engine cannot run at
> the same time on one machine** — stop one before starting the other.

---

## 2. The ML engine contract (what goes in, what comes out)

| Direction | Kafka topic | Message shape |
|---|---|---|
| **IN** | `raw-flow-features` | CICFlowMeter-style JSON: training-named columns + `src_ip`, `dst_ip`, `dst_port`, `timestamp` |
| **IN** | `suricata-alerts` | eve.json alert JSON: `src_ip`, `dest_ip`, `dest_port`, `timestamp`, `alert.{signature,category,severity,signature_id}` |
| **OUT** | `engineered-features` | ordered + median-imputed raw vector (still unscaled) + `flow_key` |
| **OUT** | `nids-alerts` | fused alert: `severity_tier`, `reasons`, `mlp{}`, `autoencoder{}`, `suricata_alert` |

**Join key** (`common.flow_key`, shared by both jobs and the producers):
`"<src_ip>-<dst_ip>-<dst_port>"` — e.g. `10.0.0.15-10.0.0.8-443`

Example `engineered-features` message:
```json
{
  "flow_key": "10.0.0.15-10.0.0.8-443",
  "src_ip": "10.0.0.15", "dst_ip": "10.0.0.8", "dst_port": 443,
  "timestamp": "2026-10-14T09:12:33+00:00",
  "feature_columns": ["Destination Port", "Flow Duration", "..."],
  "raw_features": [443.0, 1234567.0, "..."]
}
```

Example `nids-alerts` message:
```json
{
  "flow_key": "10.0.0.15-10.0.0.8-443",
  "severity_tier": "HIGH",
  "reasons": ["mlp:PortScan(0.83)", "ae:1.42x_threshold"],
  "mlp": {"label": "PortScan", "confidence": 0.83},
  "autoencoder": {"reconstruction_error": 0.021, "threshold": 0.0148},
  "suricata_alert": null
}
```

If `severity_tier` computes to `None` (quiet benign flow), nothing is emitted.

---

## 3. Connection data (copy-paste reference)

### ML engine (this folder)

| Thing | Value |
|---|---|
| Kafka bootstrap — from the host | `localhost:29092` |
| Kafka bootstrap — from inside Docker | `kafka:9092` |
| Topics | `raw-flow-features`, `suricata-alerts`, `engineered-features`, `nids-alerts` |
| Flink dashboard | http://localhost:8081 |
| Flink REST API | http://localhost:8081/overview, /jobs, /jobs/overview |
| Kafka UI | http://localhost:8080 |
| Models path (inside Flink containers) | `/opt/flink/usrlib/models` |
| Jobs path (inside Flink containers) | `/opt/flink/usrlib/flink_jobs` |
| Container names | `ml-kafka`, `ml-kafka-ui`, `ml-flink-jobmanager`, `ml-flink-taskmanager` |
| Compose project name | `nids-ml-engine` |

### Environment variables understood by the jobs (`flink_jobs/common.py`)

| Var | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka broker the jobs read/write |
| `MODELS_DIR` | `/opt/flink/usrlib/models` | Where the 8 model artifacts live |

### Downstream endpoints (FULL pipeline only — not part of the ML engine)

These exist only when you run the full `start-pipeline.ps1`, and they are where
`nids-alerts` / `engineered-features` ultimately land:

| What | URL |
|---|---|
| Backend API (Swagger UI) | http://localhost:8000/docs |
| Backend WebSocket (live alerts) | `ws://localhost:8000/ws/alerts` |
| OpenSearch (host port) | http://localhost:9400 |
| OpenSearch Dashboards | http://localhost:5601 |
| MinIO S3 API | http://localhost:9500 (`minioadmin` / `minioadmin`) |
| MinIO console | http://localhost:9501 |

> The exact MinIO/OpenSearch host ports live in the root `docker-compose.yml`;
> check it if they've been remapped.

---

## 4. Start / stop order

### The easy way

```powershell
# START  (normal, non-admin window)
powershell -ExecutionPolicy Bypass -File ".\ml-engine\start-ml-engine.ps1"

# STOP
powershell -ExecutionPolicy Bypass -File ".\ml-engine\stop-ml-engine.ps1"
```

`start-ml-engine.ps1` does, in order:
1. `docker compose up -d --build` — Kafka + Kafka-UI + Flink JobManager/TaskManager
2. Waits for Kafka to report **healthy**
3. Creates the 4 topics (idempotent)
4. Fixes Flink checkpoint directory permissions
5. Submits **feature_engineering_job** (detached)
6. Submits **fusion_job** (detached)
7. Prints the running jobs + connection data

`stop-ml-engine.ps1` cancels all `nids-*` Flink jobs, then `docker compose down`
(checkpoint data survives; use `docker compose down -v` in `ml-engine\` to wipe).

### The manual way (what the script automates)

Run these from **inside the `ml-engine\` folder**:

```powershell
# 1. Infrastructure
docker compose up -d --build

# 2. Topics (once Kafka is healthy)
foreach ($t in @("suricata-alerts","raw-flow-features","engineered-features","nids-alerts")) {
  docker exec ml-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $t --partitions 3 --replication-factor 1
}

# 3. Checkpoint permissions
docker compose exec -u root jobmanager chmod -R 777 /opt/flink/checkpoints
docker compose exec -u root taskmanager chmod -R 777 /opt/flink/checkpoints

# 4. Submit the ML jobs (order: feature engineering FIRST, fusion SECOND)
docker compose exec jobmanager flink run -d -pyFiles /opt/flink/usrlib/flink_jobs/common.py -py /opt/flink/usrlib/flink_jobs/feature_engineering_job.py
docker compose exec jobmanager flink run -d -pyFiles /opt/flink/usrlib/flink_jobs/common.py -py /opt/flink/usrlib/flink_jobs/fusion_job.py

# 5. Verify
docker compose exec jobmanager flink list
```

**Stop order** (reverse of start):
```powershell
# 1. Cancel jobs
docker compose exec jobmanager flink list          # note each nids-* job id
docker compose exec jobmanager flink cancel <job-id>

# 2. Tear down infra (keep data)
docker compose down
```

Order matters: `fusion_job` consumes `engineered-features`, which only
`feature_engineering_job` produces — so start feature-engineering first and
cancel fusion first on shutdown.

---

## 5. Testing standalone (no capture tools needed)

One-time host tooling:
```powershell
cd "D:\3rd year project\NIDS_with_kafka_and_flink"
.\venv\Scripts\Activate.ps1
pip install -r ml-engine\requirements-tools.txt
```

With the ML engine running, publish synthetic data (separate window):
```powershell
python ml-engine\tools\publish_sample_data.py --count 50
```

Watch the outputs (another window):
```powershell
python ml-engine\tools\consume_topic.py --topic engineered-features
python ml-engine\tools\consume_topic.py --topic nids-alerts
```

`publish_sample_data.py` generates rows using the real training column names from
`models/feature_columns.json`, and deliberately drops a column here and there
(20% of rows by default) so you can confirm the feature job imputes them from
`feature_medians.json`. It also emits matching Suricata alerts for every 3rd
flow (same src/dst/port) so the fusion join has something to correlate.

Useful flags: `--count`, `--interval`, `--alert-every`, `--partial-prob`,
`--bootstrap-servers`.

---

## 6. Connecting to the full pipeline

Two options:

**A. Feed live data into the ML engine's own broker.** Point the full
pipeline's host-side producers at this broker (`localhost:29092`) instead of
starting the full pipeline's Kafka:
- `producers\cicflowmeter_producer.py --bootstrap-servers localhost:29092`
- `filebeat\filebeat.yml` → `output.kafka.hosts: ["localhost:29092"]`

**B. Run only the ML engine against an external broker.** Override the
bootstrap by editing the `KAFKA_BOOTSTRAP` env on the Flink services in
`docker-compose.yml` (or set `MODELS_DIR` if you keep models elsewhere), then
re-run `start-ml-engine.ps1`.

Remember: because host ports are shared, don't run the full pipeline and the
ML engine at the same time unless you remap one of their ports.

---

## 7. Model artifacts (`models/`)

| File | Produced by | Used by |
|---|---|---|
| `neural_network_ids.h5` | `nids_mlp_pipeline.ipynb` Step 10 | fusion_job (MLP classifier) |
| `scaler.pkl` | `nids_mlp_pipeline.ipynb` Step 10 | fusion_job (MLP scaler) |
| `label_encoder.pkl` | `nids_mlp_pipeline.ipynb` Step 10 | fusion_job (decode class label) |
| `nids_autoencoder.h5` | `nids_autoencoder_train.ipynb` Step 8 | fusion_job (anomaly detection) |
| `ae_scaler.pkl` | `nids_autoencoder_train.ipynb` Step 8 | fusion_job (AE scaler) |
| `ae_threshold.pkl` | `nids_autoencoder_train.ipynb` Step 8 | fusion_job (`{"threshold": ...}`) |
| `feature_columns.json` | `scripts\save_training_metadata.py` | feature_engineering_job (column order) |
| `feature_medians.json` | `scripts\save_training_metadata.py` | feature_engineering_job (median imputation) |

To retrain/swap a model: replace the file in `models/`, then re-run
`stop-ml-engine.ps1` and `start-ml-engine.ps1` (models are bind-mounted, so no
rebuild is needed — but running jobs must be resubmitted to pick them up).

---

## 8. Tuning knobs

| Knob | Where | Meaning |
|---|---|---|
| `FUSION_TTL_SECONDS` | `flink_jobs/fusion_job.py` | How long an alert/feature waits in keyed state for a match (default 30s) |
| `determine_severity()` | `flink_jobs/fusion_job.py` | CRITICAL/HIGH/MEDIUM/LOW thresholds — tune to your risk model |
| `env.set_parallelism(2)` | both jobs | Job parallelism |
| `parallelism.default`, `numberOfTaskSlots` | `docker-compose.yml` | Scale with `docker compose up --scale taskmanager=2 -d` if backpressured |

---

## 9. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ports are not available` / bind error | Full pipeline already using 29092/8080/8081 | Stop the other stack first |
| `Failed to create directory for shared state` | Checkpoint dir owned by root | `docker compose exec -u root <svc> chmod -R 777 /opt/flink/checkpoints` |
| `ModuleNotFoundError: No module named 'flink_jobs'` | `common.py` not shipped | Keep `-pyFiles .../common.py` on the `flink run` command |
| `flink run` blocks forever | Streaming job attached | Keep the `-d` (detached) flag |
| No `nids-alerts` appearing | No alert/feature matched, or all flows scored `None` (benign) | Publish more sample data; lower thresholds in `determine_severity()` |
| `docker compose` can't find file | Run from wrong directory | The scripts `Set-Location` to their own folder; for manual runs `cd ml-engine` first |
