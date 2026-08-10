"""
Alert Indexer
=============
Consumes the `nids-alerts` topic (fusion_job.py's final output: TTL-joined
Suricata + ML-scored, severity-tiered alerts) and bulk-indexes them into
OpenSearch, so they're searchable, filterable, and dashboard-able the moment
they're produced.

Index naming: nids-alerts-YYYY.MM.DD (one index per UTC day - the standard
ELK/OpenSearch rolling-index pattern). This keeps any single index bounded
in size and makes time-based retention (delete indices older than N days)
a one-line ILM/ISM policy later, instead of a DELETE-by-query.

Document _id: an MD5 hash of (flow_key + timestamp). This makes indexing
idempotent - if this service (or the fusion job) ever reprocesses/replays
the same Kafka offset after a restart, the same alert overwrites itself
instead of appearing as a duplicate row in search results.
"""

import hashlib
import json
import logging
import signal
import time
from datetime import datetime, timezone

from dateutil import parser as date_parser
from kafka import KafkaConsumer
from opensearchpy import OpenSearch, helpers

from storage_common import (
    ALERT_INDEXER_BATCH_MAX_DOCS,
    ALERT_INDEXER_BATCH_MAX_SECONDS,
    ALERTS_INDEX_PREFIX,
    ALERTS_INDEX_TEMPLATE_NAME,
    KAFKA_BOOTSTRAP,
    OPENSEARCH_HOST,
    OPENSEARCH_PORT,
    OPENSEARCH_USE_SSL,
    TOPIC_NIDS_ALERTS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("alert_indexer")

_running = True


def _handle_shutdown(signum, frame):
    global _running
    logger.info("Shutdown signal received, flushing final batch before exit...")
    _running = False


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


# The mapping mirrors fusion_job.py's output shape exactly - see
# FusionFunction._score_and_emit() in flink_jobs/fusion_job.py. Explicit
# mappings (rather than OpenSearch's dynamic mapping) avoid IP fields being
# indexed as plain keywords, which would break CIDR/range queries later.
INDEX_TEMPLATE_BODY = {
    "index_patterns": ["{}-*".format(ALERTS_INDEX_PREFIX)],
    "template": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "flow_key": {"type": "keyword"},
                "src_ip": {"type": "ip"},
                "dst_ip": {"type": "ip"},
                "dst_port": {"type": "integer"},
                "severity_tier": {"type": "keyword"},
                "reasons": {"type": "keyword"},
                "mlp": {
                    "properties": {
                        "label": {"type": "keyword"},
                        "confidence": {"type": "float"},
                    }
                },
                "autoencoder": {
                    "properties": {
                        "reconstruction_error": {"type": "float"},
                        "threshold": {"type": "float"},
                    }
                },
                "suricata_alert": {
                    "properties": {
                        "signature": {"type": "text"},
                        "category": {"type": "keyword"},
                        "severity": {"type": "integer"},
                        "signature_id": {"type": "long"},
                    }
                },
            }
        },
    },
}


def build_client():
    return OpenSearch(
        hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
        http_compress=True,
        use_ssl=OPENSEARCH_USE_SSL,
        verify_certs=False,
        ssl_show_warn=False,
    )


def ensure_index_template(client):
    client.indices.put_index_template(name=ALERTS_INDEX_TEMPLATE_NAME, body=INDEX_TEMPLATE_BODY)
    logger.info("Index template '%s' ensured.", ALERTS_INDEX_TEMPLATE_NAME)


def index_name_for_today():
    return "{}-{}".format(ALERTS_INDEX_PREFIX, datetime.now(timezone.utc).strftime("%Y.%m.%d"))


def doc_id_for(alert):
    raw = "{}:{}".format(alert.get("flow_key", ""), alert.get("timestamp", ""))
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def normalize_timestamp(value):
    """
    Return an ISO-8601 string OpenSearch's `@timestamp: date` mapping can
    parse - or the current time if the source value is missing/unparseable.

    The `timestamp` field on a fused alert is whatever was already on the
    engineered-features record, which is CICFlowMeter's own CSV column
    ("07/08/2026 07:25:06 AM" - day-first, slash-delimited, 12-hour clock,
    not ISO-8601 at all) unless a Suricata alert supplied it instead
    (eve.json's format, which IS ISO-ish but uses a bare "+0530" offset
    with no colon - also rejected by OpenSearch's strict date parser).
    Both were being passed straight through into `@timestamp`, so every
    document was failing to index with a mapper_parsing_exception.

    dateutil.parser handles both, but NOT with the same flags: CICFlowMeter's
    slash-delimited format needs dayfirst=True (otherwise "07/08/2026"
    parses as 8 Jul instead of 7 Aug - genuinely ambiguous). Suricata's
    dash-delimited ISO format must NOT be parsed with dayfirst=True - doing
    so silently swaps month/day even on an unambiguous YYYY-MM-DD string
    (verified: dayfirst=True turned 2026-08-07 into 2026-07-08). So the
    dayfirst flag is chosen by delimiter, not applied blanket to everything.
    """
    if value:
        try:
            dt = date_parser.parse(value, dayfirst="/" in value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except (ValueError, TypeError, OverflowError):
            pass
    return datetime.now(timezone.utc).isoformat()


def to_bulk_action(alert):
    alert = dict(alert)  # don't mutate the caller's dict
    alert["@timestamp"] = normalize_timestamp(alert.get("timestamp"))
    return {
        "_index": index_name_for_today(),
        "_id": doc_id_for(alert),
        "_source": alert,
    }


def flush_batch(client, actions):
    if not actions:
        return
    success, errors = helpers.bulk(client, actions, stats_only=False, raise_on_error=False)
    if errors:
        logger.warning("%d/%d documents failed to index: %s", len(errors), len(actions), errors[:3])
    else:
        logger.info("Indexed %d alert(s).", success)


def run():
    client = build_client()
    ensure_index_template(client)

    consumer = KafkaConsumer(
        TOPIC_NIDS_ALERTS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="alert-indexer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=1000,
    )

    logger.info(
        "Watching topic '%s', indexing to '%s-*' every %d docs or %ds",
        TOPIC_NIDS_ALERTS,
        ALERTS_INDEX_PREFIX,
        ALERT_INDEXER_BATCH_MAX_DOCS,
        ALERT_INDEXER_BATCH_MAX_SECONDS,
    )

    actions = []
    last_flush = time.time()

    try:
        while _running:
            for message in consumer:
                try:
                    actions.append(to_bulk_action(message.value))
                except (TypeError, AttributeError) as e:
                    logger.warning("Skipping malformed alert: %s", e)
                    continue

                if len(actions) >= ALERT_INDEXER_BATCH_MAX_DOCS:
                    flush_batch(client, actions)
                    actions = []
                    last_flush = time.time()

                if not _running:
                    break

            if actions and (time.time() - last_flush) >= ALERT_INDEXER_BATCH_MAX_SECONDS:
                flush_batch(client, actions)
                actions = []
                last_flush = time.time()
    finally:
        flush_batch(client, actions)
        consumer.close()
        logger.info("Alert indexer stopped cleanly.")


if __name__ == "__main__":
    run()