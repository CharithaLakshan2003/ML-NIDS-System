"""
Fusion Job (PyFlink)
====================
Replaces the plain-Kafka `fusion_consumer.py`.

    suricata-alerts       --\\
                              >-- (keyed TTL join + MLP/AE scoring) --> nids-alerts
    engineered-features   --/

Design
------
Both streams are keyed by the same `flow_key` (src_ip-dst_ip-dst_port, see
common.py). A `KeyedCoProcessFunction` holds two small pieces of per-key
state, each with a TTL (default 30s, `FUSION_TTL_SECONDS` below):

  * `pending_alert`  - the most recent Suricata alert seen for this key
  * `last_feature`   - the most recent engineered-feature record seen for
                        this key (kept so a LATE-arriving Suricata alert can
                        still be correlated with a flow that already scored)

This reproduces the "TTL-cache join" from the plain-Kafka version, but as
Flink-managed keyed state instead of an in-process dict, so it survives
task restarts (state is checkpointed) and scales across parallel subtasks.

Every engineered-feature record is scored by BOTH models the moment it
arrives (the anomaly-detection side does not need a Suricata match to fire -
that's the whole point of the unsupervised autoencoder catching things
signatures don't). If a Suricata alert for the same key is already cached,
it's folded in immediately; otherwise the alert can still catch up later
via `pending_alert` within the TTL window, or the feature stays available
via `last_feature` for an alert arriving slightly late.

Severity tiering
----------------
The exact tier thresholds below are a reasonable STARTING POINT, not a
reproduction of any specific prior rule set - tune `determine_severity()`
to match whatever tiers/thresholds your risk model actually needs.
"""

import json
import logging
import sys

import numpy as np

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
)
from pyflink.datastream.functions import KeyedCoProcessFunction, MapFunction
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

sys.path.append("/opt/flink/usrlib/flink_jobs")
from common import (  # noqa: E402
    KAFKA_BOOTSTRAP,
    MODELS_DIR,
    TOPIC_SURICATA_ALERTS,
    TOPIC_ENGINEERED_FEATURES,
    TOPIC_NIDS_ALERTS,
    flow_key,
)

logger = logging.getLogger("fusion_job")

FUSION_TTL_SECONDS = 30  # how long an alert/feature waits in state for a match


# ---------------------------------------------------------------------------
# Stream 1: normalize raw Suricata eve.json alert events (as shipped verbatim
# by Filebeat) down to the fields the fusion function needs.
# ---------------------------------------------------------------------------
class AlertNormalizer(MapFunction):
    def map(self, raw_json):
        try:
            evt = json.loads(raw_json)
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning("Dropping unparsable Suricata event: %s", e)
            return None

        alert = evt.get("alert", {}) or {}
        src_ip = evt.get("src_ip")
        dst_ip = evt.get("dest_ip")
        dst_port = evt.get("dest_port")

        out = {
            "flow_key": flow_key(src_ip, dst_ip, dst_port),
            "src_ip": src_ip,
            "dst_ip": dst_ip,
            "dst_port": dst_port,
            "timestamp": evt.get("timestamp"),
            "signature": alert.get("signature"),
            "category": alert.get("category"),
            "severity": alert.get("severity"),
            "signature_id": alert.get("signature_id"),
        }
        return json.dumps(out)


# ---------------------------------------------------------------------------
# Severity tiering - see module docstring. Pure function, easy to unit test
# or replace independently of the Flink plumbing around it.
# ---------------------------------------------------------------------------
def determine_severity(mlp_label, mlp_confidence, ae_error, ae_threshold, suricata_alert):
    ae_ratio = (ae_error / ae_threshold) if ae_threshold and ae_threshold > 0 else 0.0
    mlp_is_attack = str(mlp_label).upper() != "BENIGN"
    reasons = []

    if suricata_alert:
        reasons.append("suricata:{}".format(suricata_alert.get("signature")))
    if mlp_is_attack:
        reasons.append("mlp:{}({:.2f})".format(mlp_label, mlp_confidence))
    if ae_ratio >= 0.8:
        reasons.append("ae:{:.2f}x_threshold".format(ae_ratio))

    if suricata_alert and (mlp_is_attack or ae_ratio >= 2.0):
        tier = "CRITICAL"
    elif suricata_alert or (mlp_is_attack and mlp_confidence >= 0.7) or ae_ratio >= 2.0:
        tier = "HIGH"
    elif (mlp_is_attack and mlp_confidence >= 0.4) or ae_ratio >= 1.0:
        tier = "MEDIUM"
    elif ae_ratio >= 0.8:
        tier = "LOW"
    else:
        tier = None  # nothing worth emitting - quiet, unremarkable benign traffic

    return tier, reasons


# ---------------------------------------------------------------------------
# The core fusion logic: keyed TTL-cache join + dual-model scoring.
# ---------------------------------------------------------------------------
class FusionFunction(KeyedCoProcessFunction):
    def open(self, runtime_context):
        import joblib
        import tensorflow as tf

        self.mlp_model = tf.keras.models.load_model(
            "{}/neural_network_ids.h5".format(MODELS_DIR), compile=False
        )
        self.mlp_scaler = joblib.load("{}/scaler.pkl".format(MODELS_DIR))
        self.label_encoder = joblib.load("{}/label_encoder.pkl".format(MODELS_DIR))

        self.ae_model = tf.keras.models.load_model(
            "{}/nids_autoencoder.h5".format(MODELS_DIR), compile=False
        )
        self.ae_scaler = joblib.load("{}/ae_scaler.pkl".format(MODELS_DIR))
        ae_meta = joblib.load("{}/ae_threshold.pkl".format(MODELS_DIR))
        self.ae_threshold = ae_meta["threshold"]

        ttl_config = (
            StateTtlConfig.new_builder(Time.seconds(FUSION_TTL_SECONDS))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanup_incrementally(cleanup_size=10, run_cleanup_for_every_record=True)
            .build()
        )

        alert_descriptor = ValueStateDescriptor("pending_alert", Types.STRING())
        alert_descriptor.enable_time_to_live(ttl_config)
        self.pending_alert = runtime_context.get_state(alert_descriptor)

        feature_descriptor = ValueStateDescriptor("last_feature", Types.STRING())
        feature_descriptor.enable_time_to_live(ttl_config)
        self.last_feature = runtime_context.get_state(feature_descriptor)

    def _score_and_emit(self, feature_record, suricata_alert):
        raw_features = np.array(feature_record["raw_features"], dtype=np.float64).reshape(1, -1)

        # MLP: its own scaler, softmax over classes, decode the winning label.
        mlp_scaled = self.mlp_scaler.transform(raw_features)
        mlp_proba = self.mlp_model.predict(mlp_scaled, verbose=0)[0]
        mlp_idx = int(np.argmax(mlp_proba))
        mlp_confidence = float(mlp_proba[mlp_idx])
        mlp_label = self.label_encoder.inverse_transform([mlp_idx])[0]

        # Autoencoder: its own (benign-only-fit) scaler, reconstruction MSE.
        ae_scaled = self.ae_scaler.transform(raw_features)
        ae_recon = self.ae_model.predict(ae_scaled, verbose=0)
        ae_error = float(np.mean(np.square(ae_scaled - ae_recon)))

        tier, reasons = determine_severity(
            mlp_label, mlp_confidence, ae_error, self.ae_threshold, suricata_alert
        )
        if tier is None:
            return  # quiet benign flow, nothing corroborating an issue - skip

        out = {
            "flow_key": feature_record["flow_key"],
            "src_ip": feature_record.get("src_ip"),
            "dst_ip": feature_record.get("dst_ip"),
            "dst_port": feature_record.get("dst_port"),
            "timestamp": feature_record.get("timestamp"),
            "severity_tier": tier,
            "reasons": reasons,
            "mlp": {"label": str(mlp_label), "confidence": mlp_confidence},
            "autoencoder": {
                "reconstruction_error": ae_error,
                "threshold": self.ae_threshold,
            },
            "suricata_alert": suricata_alert,  # None if no signature match
        }
        yield json.dumps(out)

    def process_element1(self, value, ctx):
        """Input 1 = suricata-alerts (normalized)."""
        if value is None:
            return
        alert = json.loads(value)
        self.pending_alert.update(value)

        cached_feature = self.last_feature.value()
        if cached_feature is not None:
            feature_record = json.loads(cached_feature)
            for out in self._score_and_emit(feature_record, alert):
                yield out

    def process_element2(self, value, ctx):
        """Input 2 = engineered-features."""
        if value is None:
            return
        feature_record = json.loads(value)
        self.last_feature.update(value)

        cached_alert_raw = self.pending_alert.value()
        cached_alert = json.loads(cached_alert_raw) if cached_alert_raw is not None else None

        for out in self._score_and_emit(feature_record, cached_alert):
            yield out


def build_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.enable_checkpointing(30000)

    alert_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(TOPIC_SURICATA_ALERTS)
        .set_group_id("fusion-job-alerts")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    feature_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(TOPIC_ENGINEERED_FEATURES)
        .set_group_id("fusion-job-features")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(TOPIC_NIDS_ALERTS)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    no_wm = WatermarkStrategy.no_watermarks()

    alerts = env.from_source(alert_source, no_wm, "suricata-alerts-source").map(
        AlertNormalizer(), output_type=Types.STRING()
    )
    features = env.from_source(feature_source, no_wm, "engineered-features-source")

    keyed_alerts = alerts.filter(lambda x: x is not None).key_by(
        lambda x: json.loads(x)["flow_key"], key_type=Types.STRING()
    )
    keyed_features = features.key_by(
        lambda x: json.loads(x)["flow_key"], key_type=Types.STRING()
    )

    fused = keyed_alerts.connect(keyed_features).process(
        FusionFunction(), output_type=Types.STRING()
    ).name("fusion-ttl-join-and-scoring")

    fused.sink_to(sink).name("nids-alerts-sink")

    env.execute("nids-fusion-job")


if __name__ == "__main__":
    build_pipeline()
