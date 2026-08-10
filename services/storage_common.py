"""
Shared configuration for the storage-layer services (datalake_writer.py,
alert_indexer.py) and the FastAPI backend. Mirrors the pattern in
flink_jobs/common.py: env-var overridable, one source of truth.
"""

import os

# --- Kafka ---------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")

TOPIC_ENGINEERED_FEATURES = "engineered-features"
TOPIC_NIDS_ALERTS = "nids-alerts"

# --- MinIO (S3-compatible object storage) - the feature-engineering data lake
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "nids-datalake")

# Batching for the Parquet writer: whichever limit is hit first triggers a
# flush. Small defaults so you see files appear quickly in dev; raise both
# for production to get fewer, larger Parquet files.
DATALAKE_BATCH_MAX_ROWS = int(os.environ.get("DATALAKE_BATCH_MAX_ROWS", "2000"))
DATALAKE_BATCH_MAX_SECONDS = int(os.environ.get("DATALAKE_BATCH_MAX_SECONDS", "60"))

# --- OpenSearch - the alert search/log store ------------------------------
OPENSEARCH_HOST = os.environ.get("OPENSEARCH_HOST", "opensearch")
OPENSEARCH_PORT = int(os.environ.get("OPENSEARCH_PORT", "9200"))
OPENSEARCH_USE_SSL = os.environ.get("OPENSEARCH_USE_SSL", "false").lower() == "true"

# Alerts are indexed into a daily-rolling index, the standard ELK/OpenSearch
# pattern (nids-alerts-2026.08.01) - keeps any one index from growing
# unbounded and makes time-based retention/ILM straightforward later.
ALERTS_INDEX_PREFIX = os.environ.get("ALERTS_INDEX_PREFIX", "nids-alerts")
ALERTS_INDEX_TEMPLATE_NAME = "nids-alerts-template"

ALERT_INDEXER_BATCH_MAX_DOCS = int(os.environ.get("ALERT_INDEXER_BATCH_MAX_DOCS", "200"))
ALERT_INDEXER_BATCH_MAX_SECONDS = int(os.environ.get("ALERT_INDEXER_BATCH_MAX_SECONDS", "5"))
