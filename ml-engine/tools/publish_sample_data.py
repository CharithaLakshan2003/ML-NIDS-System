"""
Sample-data publisher for the standalone ML engine.
===================================================

Lets you exercise the ML engine (feature_engineering_job + fusion_job) with
NO Suricata, Filebeat, or CICFlowMeter running. It publishes, to the same
Kafka broker the jobs read from:

  * `raw-flow-features` - synthetic CICFlowMeter-style rows, generated using
    the exact training column names in models/feature_columns.json (so the
    feature-engineering job finds every column it looks for), plus the
    normalized join-key fields (src_ip / dst_ip / dst_port / timestamp).
  * `suricata-alerts`   - matching Suricata eve.json-style alert events for a
    subset of those flows, using the SAME (src_ip, dst_ip, dst_port) triple so
    the fusion job's keyed TTL join can correlate them.

Some raw rows are deliberately published with a column or two missing so you
can watch the feature job impute them from feature_medians.json.

Usage
-----
    .\\venv\\Scripts\\Activate.ps1
    pip install -r ml-engine\\requirements-tools.txt
    python ml-engine\\tools\\publish_sample_data.py --count 50

Outputs land on the `engineered-features` and `nids-alerts` topics - watch
them with:
    python ml-engine\\tools\\consume_topic.py --topic nids-alerts
or via the Kafka UI at http://localhost:8080.
"""

import argparse
import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_COLUMNS = os.path.normpath(os.path.join(HERE, "..", "models", "feature_columns.json"))

TOPIC_RAW_FLOW_FEATURES = "raw-flow-features"
TOPIC_SURICATA_ALERTS = "suricata-alerts"

SRC_IPS = ["10.0.0.15", "10.0.0.27", "192.168.1.50", "172.16.0.9", "203.0.113.7", "198.51.100.23"]
DST_IPS = ["10.0.0.5", "10.0.0.8", "8.8.8.8", "142.250.190.14"]
DST_PORTS = [80, 443, 22, 3389, 53, 445, 8080]

SIGNATURES = [
    ("ET SCAN Potential SSH Scan", "Attempted Information Leak", 2, 2001219),
    ("ET POLICY Suspicious inbound to mySQL port", "Potentially Bad Traffic", 3, 2010935),
    ("ET MALWARE Possible C2 Beacon", "A Network Trojan was detected", 1, 2027757),
    ("ET EXPLOIT Possible SMB Attack", "Attempted Administrator Privilege Gain", 1, 2024297),
]


def make_value(col, dst_port):
    """Produce a plausible numeric value for a CICFlowMeter column."""
    low = col.lower()
    if col == "Destination Port":
        return dst_port
    if "port" in low:
        return random.choice(DST_PORTS)
    if "flag count" in low or low.startswith(
        ("fin ", "syn ", "rst ", "psh ", "ack ", "urg ", "cwe ", "ece ")
    ):
        return random.randint(0, 3)
    if low.endswith("/s") or any(k in low for k in ("bytes", "length", "size", "iat", "duration")):
        return round(random.uniform(0, 5000), 3)
    return round(random.uniform(0, 100), 3)


def build_feature_row(columns, src_ip, dst_ip, dst_port, partial):
    """One CICFlowMeter-style JSON row in the shape feature_engineering_job expects."""
    drop = set()
    if partial:
        drop = set(random.sample(columns, k=min(len(columns), random.randint(1, 2))))

    row = {
        "src_ip": src_ip,
        "dst_ip": dst_ip,
        "dst_port": dst_port,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    for col in columns:
        if col not in drop:
            row[col] = make_value(col, dst_port)
    return row


def build_suricata_alert(src_ip, dst_ip, dst_port):
    """One eve.json-style alert event in the shape AlertNormalizer expects."""
    signature, category, severity, signature_id = random.choice(SIGNATURES)
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": "alert",
        "src_ip": src_ip,
        "dest_ip": dst_ip,
        "dest_port": dst_port,
        "alert": {
            "signature": signature,
            "category": category,
            "severity": severity,
            "signature_id": signature_id,
        },
    }


def main():
    parser = argparse.ArgumentParser(description="Publish synthetic flows/alerts to the ML engine.")
    parser.add_argument("--bootstrap-servers", default="localhost:29092")
    parser.add_argument("--columns", default=DEFAULT_COLUMNS, help="Path to feature_columns.json")
    parser.add_argument("--count", type=int, default=50, help="Number of flow rows to publish")
    parser.add_argument("--interval", type=float, default=0.2, help="Seconds between rows")
    parser.add_argument("--alert-every", type=int, default=3,
                        help="Emit a Suricata alert for every Nth flow (same join key)")
    parser.add_argument("--partial-prob", type=float, default=0.2,
                        help="Probability a raw row has missing columns (tests median imputation)")
    args = parser.parse_args()

    with open(args.columns, "r") as f:
        columns = json.load(f)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        linger_ms=50,
    )

    print("Publishing {} flows to '{}' (bootstrap {}), alert every {}...".format(
        args.count, TOPIC_RAW_FLOW_FEATURES, args.bootstrap_servers, args.alert_every))

    sent_flows = 0
    sent_alerts = 0
    for i in range(args.count):
        src_ip = random.choice(SRC_IPS)
        dst_ip = random.choice(DST_IPS)
        dst_port = random.choice(DST_PORTS)
        key = "{}-{}-{}".format(src_ip, dst_ip, dst_port)

        partial = random.random() < args.partial_prob
        row = build_feature_row(columns, src_ip, dst_ip, dst_port, partial)
        producer.send(TOPIC_RAW_FLOW_FEATURES, key=key, value=row)
        sent_flows += 1

        if args.alert_every > 0 and (i + 1) % args.alert_every == 0:
            alert = build_suricata_alert(src_ip, dst_ip, dst_port)
            producer.send(TOPIC_SURICATA_ALERTS, key=key, value=alert)
            sent_alerts += 1

        if args.interval:
            time.sleep(args.interval)

    producer.flush()
    producer.close()
    print("Done. Flows: {}, Suricata alerts: {}.".format(sent_flows, sent_alerts))
    print("Now watch: python ml-engine\\tools\\consume_topic.py --topic nids-alerts")


if __name__ == "__main__":
    main()
