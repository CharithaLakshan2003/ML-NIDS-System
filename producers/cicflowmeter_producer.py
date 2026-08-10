"""
CICFlowMeter CSV -> Kafka producer.

Tails CICFlowMeter's output directory (it writes one CSV, rotating to a new
file periodically) and publishes each completed flow row as JSON to the
`raw-flow-features` topic, keyed by the same flow_key the Flink jobs use for
joining later.

Handles:
  * File rotation - CICFlowMeter starts a new CSV periodically; we detect
    new files appearing in the watch directory and switch to tailing them.
  * Partial/malformed rows - a row read while CICFlowMeter is mid-write can
    have the wrong column count or non-numeric garbage in a numeric field.
    Such rows are logged and skipped here; anything that DOES parse but has
    a few missing/garbage fields is still forwarded - the Flink feature
    engineering job is responsible for imputing those via training medians,
    since it's the one with the training-time statistics.

Run: python producers/cicflowmeter_producer.py --watch-dir /path/to/cicflowmeter/output
"""

import argparse
import csv
import io
import json
import logging
import os
import time

from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("cicflowmeter_producer")

TOPIC = "raw-flow-features"

# CICFlowMeter's raw header names -> the lowercase keys the rest of the
# pipeline (Flink jobs, common.flow_key) expect for the join-key fields.
# All other columns are forwarded as-is under their original header name;
# feature_columns.json (from save_training_metadata.py) is the source of
# truth for which of those the models actually use.
KEY_FIELD_MAP = {
    "Src IP": "src_ip",
    "Dst IP": "dst_ip",
    "Dst Port": "dst_port",
    "Timestamp": "timestamp",
}


def normalize_row(row):
    """Map CICFlowMeter's header names onto the fields downstream needs,
    keeping every other column under its original name so the Flink feature
    engineering job can look values up by the training-time column names."""
    out = dict(row)
    for src_name, dst_name in KEY_FIELD_MAP.items():
        if src_name in out:
            out[dst_name] = out.pop(src_name)
    return out


def find_latest_csv(watch_dir):
    candidates = [f for f in os.listdir(watch_dir) if f.lower().endswith(".csv")]
    if not candidates:
        return None
    candidates.sort(key=lambda f: os.path.getmtime(os.path.join(watch_dir, f)))
    return os.path.join(watch_dir, candidates[-1])


def tail_csv(watch_dir, producer, poll_interval=1.0):
    current_path = None
    fh = None
    reader = None
    header = None
    inode = None

    while True:
        latest = find_latest_csv(watch_dir)

        # Rotation detection: a newer file appeared, or the current file's
        # inode changed (rewritten in place by some CICFlowMeter configs).
        if latest and (latest != current_path):
            if fh:
                fh.close()
            logger.info("Tailing new file: %s", latest)
            fh = open(latest, "r", newline="", encoding="utf-8", errors="replace")
            current_path = latest
            inode = os.fstat(fh.fileno()).st_ino
            header = None
            reader = None

        if fh is None:
            time.sleep(poll_interval)
            continue

        # Detect in-place rewrite (rotation without a new filename).
        try:
            if os.stat(current_path).st_ino != inode:
                fh.close()
                fh = open(current_path, "r", newline="", encoding="utf-8", errors="replace")
                inode = os.fstat(fh.fileno()).st_ino
                header = None
        except FileNotFoundError:
            current_path = None
            time.sleep(poll_interval)
            continue

        line = fh.readline()
        if not line:
            time.sleep(poll_interval)
            continue

        if header is None:
            header = next(csv.reader(io.StringIO(line)))
            reader = csv.reader([])  # placeholder; we parse line-by-line below
            continue

        try:
            parsed = next(csv.reader(io.StringIO(line)))
        except csv.Error as e:
            logger.warning("Skipping malformed CSV line (parse error): %s", e)
            continue

        if len(parsed) != len(header):
            # Classic symptom of reading a row CICFlowMeter hadn't finished
            # writing yet. Skip rather than guess at padding/truncating.
            logger.warning(
                "Skipping partial row: expected %d fields, got %d", len(header), len(parsed)
            )
            continue

        row = dict(zip(header, parsed))
        row = normalize_row(row)

        key = "{}-{}-{}".format(
            row.get("src_ip", ""), row.get("dst_ip", ""), row.get("dst_port", "")
        )
        try:
            producer.send(TOPIC, key=key.encode("utf-8"), value=row)
        except Exception as e:  # noqa: BLE001 - producer errors shouldn't kill the tail loop
            logger.error("Kafka send failed, dropping row: %s", e)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--watch-dir", required=True, help="Directory CICFlowMeter writes CSVs into")
    parser.add_argument("--bootstrap-servers", default="localhost:29092")
    parser.add_argument("--poll-interval", type=float, default=1.0)
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
    )

    logger.info("Watching %s, publishing to topic '%s'", args.watch_dir, TOPIC)
    try:
        tail_csv(args.watch_dir, producer, poll_interval=args.poll_interval)
    except KeyboardInterrupt:
        logger.info("Shutting down.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
