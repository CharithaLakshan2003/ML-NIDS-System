"""
Data Lake Writer
================
Consumes the `engineered-features` topic (the cleaned, ordered, imputed -
but not yet model-scaled - flow feature vectors from feature_engineering_job.py)
and writes them as Parquet files into MinIO, partitioned by date/hour:

    s3://nids-datalake/engineered-features/dt=2026-08-01/hour=14/part-<uuid>.parquet

This is the cold-storage / data-lake side of the pipeline: the same feature
vectors the fusion job scores in real time are also durably archived here in
a columnar format, so they can be queried later with any Parquet-aware
engine (DuckDB, Spark, Athena-on-MinIO, pandas) for offline analysis,
retraining, or auditing - independent of Kafka's retention window.

Each engineered-features message looks like:
    {
      "flow_key": "...", "src_ip": "...", "dst_ip": "...", "dst_port": ...,
      "timestamp": "...",
      "feature_columns": ["Flow Duration", "Total Fwd Packets", ...],
      "raw_features": [1234.0, 12.0, ...]
    }

`feature_columns` + `raw_features` are zipped into named columns so the
Parquet file has a real, self-describing schema rather than opaque arrays.
"""

import io
import json
import logging
import signal
import time
import uuid
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from kafka import KafkaConsumer

from storage_common import (
    DATALAKE_BATCH_MAX_ROWS,
    DATALAKE_BATCH_MAX_SECONDS,
    KAFKA_BOOTSTRAP,
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    TOPIC_ENGINEERED_FEATURES,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("datalake_writer")

_running = True


def _handle_shutdown(signum, frame):
    global _running
    logger.info("Shutdown signal received, flushing final batch before exit...")
    _running = False


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


def build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def ensure_bucket(s3):
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
    except ClientError:
        logger.info("Bucket '%s' not found - creating it.", MINIO_BUCKET)
        s3.create_bucket(Bucket=MINIO_BUCKET)


def flatten_record(msg):
    """One engineered-features Kafka message -> one flat dict (one Parquet row)."""
    row = {
        "flow_key": msg.get("flow_key"),
        "src_ip": msg.get("src_ip"),
        "dst_ip": msg.get("dst_ip"),
        "dst_port": msg.get("dst_port"),
        "timestamp": msg.get("timestamp"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    columns = msg.get("feature_columns") or []
    values = msg.get("raw_features") or []
    for name, value in zip(columns, values):
        # Parquet/Arrow needs stable column names - spaces and slashes in
        # CICFlowMeter's original headers are replaced so every engine can
        # query them without quoting gymnastics.
        safe_name = str(name).strip().replace(" ", "_").replace("/", "_per_")
        row[safe_name] = value
    return row


def write_batch_to_minio(s3, rows):
    if not rows:
        return
    table = pa.Table.from_pylist(rows)

    now = datetime.now(timezone.utc)
    key = "engineered-features/dt={}/hour={:02d}/part-{}.parquet".format(
        now.strftime("%Y-%m-%d"), now.hour, uuid.uuid4().hex
    )

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    s3.upload_fileobj(buf, MINIO_BUCKET, key)
    logger.info("Wrote %d rows -> s3://%s/%s", len(rows), MINIO_BUCKET, key)


def run():
    s3 = build_s3_client()
    ensure_bucket(s3)

    consumer = KafkaConsumer(
        TOPIC_ENGINEERED_FEATURES,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="datalake-writer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=1000,  # so the poll loop wakes up to check the time-based flush
    )

    logger.info(
        "Watching topic '%s', flushing to s3://%s every %d rows or %ds",
        TOPIC_ENGINEERED_FEATURES,
        MINIO_BUCKET,
        DATALAKE_BATCH_MAX_ROWS,
        DATALAKE_BATCH_MAX_SECONDS,
    )

    batch = []
    last_flush = time.time()

    try:
        while _running:
            for message in consumer:
                try:
                    batch.append(flatten_record(message.value))
                except (TypeError, AttributeError) as e:
                    logger.warning("Skipping malformed message: %s", e)
                    continue

                if len(batch) >= DATALAKE_BATCH_MAX_ROWS:
                    write_batch_to_minio(s3, batch)
                    batch = []
                    last_flush = time.time()

                if not _running:
                    break

            if batch and (time.time() - last_flush) >= DATALAKE_BATCH_MAX_SECONDS:
                write_batch_to_minio(s3, batch)
                batch = []
                last_flush = time.time()
    finally:
        write_batch_to_minio(s3, batch)  # don't lose the last partial batch
        consumer.close()
        logger.info("Data lake writer stopped cleanly.")


if __name__ == "__main__":
    run()
