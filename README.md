# Hybrid NIDS - Apache Flink Edition

PyFlink replacement for the two Kafka consumer scripts in the plain-Kafka
version of this pipeline. Suricata (signatures) and CICFlowMeter (flow
features) still feed Kafka the same way; what changes is everything
downstream of Kafka now runs as two PyFlink jobs on a real Flink cluster
instead of two standalone Python processes.

```
Suricata --eve.json--> Filebeat --> [suricata-alerts]  --\
                                                            >-- fusion_job.py --> [nids-alerts]
CICFlowMeter --CSV--> cicflowmeter_producer.py --> [raw-flow-features]
                                                          |
                                          feature_engineering_job.py
                                                          |
                                                          v
                                              [engineered-features] ---/
```

## Files

| File | Role |
|---|---|
| `docker-compose.yml` | Kafka (KRaft) + Kafka UI (`localhost:8080`) + Flink JobManager/TaskManager (`localhost:8081`) |
| `Dockerfile.flink` | Flink 1.18 image + PyFlink, TensorFlow, scikit-learn, and the Kafka connector jar |
| `scripts/create_topics.sh` | Creates the 4 topics |
| `filebeat/filebeat.yml` | Tails `eve.json`, filters to `event_type: alert`, ships to Kafka |
| `producers/cicflowmeter_producer.py` | Tails CICFlowMeter's output CSV (handles rotation + malformed partial rows), publishes to Kafka |
| `flink_jobs/common.py` | Shared constants + the `flow_key()` join-key format both jobs must agree on |
| `flink_jobs/feature_engineering_job.py` | **Job 1** - cleans/orders/imputes using training-time medians |
| `flink_jobs/fusion_job.py` | **Job 2** - keyed TTL join + MLP/AE scoring + severity tiers |
| `scripts/save_training_metadata.py` | Notebook cell that generates `feature_columns.json` / `feature_medians.json` |
| `requirements.txt` | Deps for the Flink image + the host-side producer/notebook cell |

## One-time: new artifacts your `models/` folder doesn't have yet

Same requirement as the plain-Kafka version, for the same reason: the
feature-engineering job cannot safely guess column order or impute a
missing value from a single streaming row.

1. Open `nids_mlp_pipeline.ipynb`.
2. Paste the contents of `scripts/save_training_metadata.py` as a new cell
   right after **Step 4** (train/test split) and before **Step 5** (scaling).
3. Run it. This produces `models/feature_columns.json` and
   `models/feature_medians.json`.

## Populate `models/`

Copy these 8 files into this project's `models/` directory - both Flink
jobs load them via a bind mount (see `docker-compose.yml`), so no rebuild
is needed if you retrain and swap files later:

```
models/
├── neural_network_ids.h5      (from nids_mlp_pipeline.ipynb, Step 10)
├── scaler.pkl                 (from nids_mlp_pipeline.ipynb, Step 10)
├── label_encoder.pkl          (from nids_mlp_pipeline.ipynb, Step 10)
├── feature_columns.json       (from save_training_metadata.py, above)
├── feature_medians.json       (from save_training_metadata.py, above)
├── nids_autoencoder.h5        (from nids_autoencoder_train.ipynb, Step 8)
├── ae_scaler.pkl              (from nids_autoencoder_train.ipynb, Step 8)
└── ae_threshold.pkl           (from nids_autoencoder_train.ipynb, Step 8)
```

## Run order

```bash
# 1. Bring up Kafka, Kafka UI, and the Flink cluster
docker compose up -d --build

# 2. Create the 4 topics
./scripts/create_topics.sh

# 3. Start Suricata against your interface/pcap as usual (not part of this
#    compose file - Suricata typically runs on the box actually seeing
#    traffic, which may not be this Docker host)

# 4. Start Filebeat, pointed at this repo's filebeat/filebeat.yml
filebeat -e -c filebeat/filebeat.yml
#    If Filebeat runs on the SAME host as this docker-compose.yml, change
#    `output.kafka.hosts` in filebeat.yml to `localhost:29092`. If it runs
#    on a different host, use that host's reachable address for Kafka's
#    9092 listener instead.

# 5. Start CICFlowMeter against the same interface/pcap, writing to some
#    output directory, e.g. ./cicflowmeter-output

# 6. Start its producer (on the host, not in Docker - it needs the
#    KafkaProducer python client and read access to CICFlowMeter's output dir)
pip install -r requirements.txt
python producers/cicflowmeter_producer.py \
    --watch-dir ./cicflowmeter-output \
    --bootstrap-servers localhost:29092

# 7. Submit the feature engineering job to the Flink cluster
docker compose exec jobmanager flink run -py /opt/flink/usrlib/flink_jobs/feature_engineering_job.py

# 8. Submit the fusion job
docker compose exec jobmanager flink run -py /opt/flink/usrlib/flink_jobs/fusion_job.py
```

Watch progress at:
- **Kafka UI**: http://localhost:8080 - see messages flowing through each topic
- **Flink dashboard**: http://localhost:8081 - see both jobs running, checkpoint status, backpressure, and per-operator throughput

## Tuning knobs worth knowing about

- **`FUSION_TTL_SECONDS`** in `fusion_job.py` (default 30s) - how long an
  alert or feature record waits in Flink keyed state for a match on the
  other stream before it's evicted. Widen this if Suricata alerts and
  CICFlowMeter flow completion are far apart in your setup.
- **`determine_severity()`** in `fusion_job.py` - the tier thresholds
  (CRITICAL/HIGH/MEDIUM/LOW) are a reasonable starting point, not a fixed
  spec. Tune the confidence/ratio cutoffs to match your actual risk model.
- **`parallelism.default` / `taskmanager.numberOfTaskSlots`** in
  `docker-compose.yml` - scale out with `docker compose up --scale
  taskmanager=2 -d` if either job shows backpressure on the dashboard.
- **`env.enable_checkpointing(30000)`** in both jobs - checkpoint interval
  in ms. Lower it if you need faster recovery after a TaskManager restart;
  raise it if checkpointing itself shows up as overhead on the dashboard.

## Honest limitations

I could not run this against a real Kafka broker, a real Flink cluster, or
real Suricata/CICFlowMeter output in this environment (no Docker/network
access here). I checked every Python file for syntax correctness
(`py_compile`) and validated the compose file parses, but the PyFlink
Kafka-connector wiring, the keyed-state TTL join, and the model-loading
paths in `fusion_job.py` have **not** been exercised end-to-end. Before
trusting this unattended:

1. Watch the Flink dashboard for both jobs reaching RUNNING with no
   restarts in the first few minutes.
2. Watch `engineered-features` and `nids-alerts` in Kafka UI for the first
   batch of real messages and sanity-check the JSON shape.
3. Confirm `fusion_job.py`'s `open()` actually finds all 8 files under
   `models/` - a missing/misnamed file surfaces as a task failure with a
   `FileNotFoundError` in the TaskManager logs, not a silent no-op.
