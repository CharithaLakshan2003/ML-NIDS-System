"""
Shared constants/helpers for the Hybrid NIDS PyFlink jobs.

Both feature_engineering_job.py and fusion_job.py import this so the topic
names, join-key format, and Kafka bootstrap address stay in sync in exactly
one place.
"""

import math
import os

# --- Kafka -------------------------------------------------------------
# Inside the Flink containers this resolves via the "kafka" service name
# (see docker-compose.yml). Override with KAFKA_BOOTSTRAP if you run a job
# outside Docker against a different broker.
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")

TOPIC_SURICATA_ALERTS = "suricata-alerts"
TOPIC_RAW_FLOW_FEATURES = "raw-flow-features"
TOPIC_ENGINEERED_FEATURES = "engineered-features"
TOPIC_NIDS_ALERTS = "nids-alerts"

# --- Model artifact paths (bind-mounted into the containers, see
#     docker-compose.yml) -------------------------------------------------
MODELS_DIR = os.environ.get("MODELS_DIR", "/opt/flink/usrlib/models")


def flow_key(src_ip, dst_ip, dst_port):
    """
    Canonical join key shared by the Suricata alert stream and the
    engineered-feature stream. Both producers (Filebeat's eve.json shipping
    and cicflowmeter_producer.py / feature_engineering_job.py) must agree on
    this exact format, or the fusion job's keyed join will never match.

    dst_port is coerced to str/int defensively since eve.json emits it as a
    JSON number while some CICFlowMeter CSV dumps emit it as a numeric
    string with possible trailing whitespace.
    """
    try:
        port = int(float(dst_port))
    except (TypeError, ValueError):
        port = -1
    return "{}-{}-{}".format(str(src_ip).strip(), str(dst_ip).strip(), port)


def safe_float(value, default=0.0):
    """
    Coerce a single streaming value to a finite float, falling back to
    `default` (the caller passes in the training-time median) for anything
    that isn't a clean finite number: None, empty string, non-numeric
    garbage from a malformed/partial CSV row, or inf/-inf/NaN. A lone
    streaming record has no population to recompute a median from, which is
    exactly why feature_medians.json exists - see save_training_metadata.py.
    """
    if value is None:
        return default
    try:
        f = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(f) or math.isinf(f):
        return default
    return f
