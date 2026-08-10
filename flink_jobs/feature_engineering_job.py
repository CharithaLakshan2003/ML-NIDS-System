"""
Feature Engineering Job (PyFlink)
=================================
Replaces the plain-Kafka `feature_engineering_consumer.py`.

    raw-flow-features  --(this job)-->  engineered-features

For every CICFlowMeter flow record published by cicflowmeter_producer.py:
  1. Parse the JSON row.
  2. Reorder/select columns to match `feature_columns.json` (the exact
     column order the models were trained on - CICFlowMeter's own column
     order is not guaranteed to match, and Kafka JSON has no schema to
     enforce it).
  3. For any column that's missing, null, or non-numeric (partial/malformed
     row from a CSV read mid-write), impute with that column's TRAINING-TIME
     median from `feature_medians.json`. A single streaming row has no
     population of its own to compute a median from - the medians must come
     from training, which is why save_training_metadata.py exists.
  4. Attach the join key (src_ip/dst_ip/dst_port) and forward the ordered,
     imputed, but still UNSCALED raw feature vector downstream.

Scaling is deliberately NOT done here: the MLP and autoencoder each have
their own StandardScaler fit on different data (all-classes vs. benign-only),
so the fusion job applies each model's own scaler right before that model's
predict() call, using this same ordered raw vector for both.
"""

import json
import logging
import sys

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
)
from pyflink.datastream.functions import MapFunction

sys.path.append("/opt/flink/usrlib/flink_jobs")
from common import (  # noqa: E402
    KAFKA_BOOTSTRAP,
    MODELS_DIR,
    TOPIC_RAW_FLOW_FEATURES,
    TOPIC_ENGINEERED_FEATURES,
    flow_key,
    safe_float,
)

logger = logging.getLogger("feature_engineering_job")


class FeatureEngineer(MapFunction):
    """
    RichMapFunction-style feature engineer. Loads feature_columns.json and
    feature_medians.json once per task-slot in open(), not per record.
    """

    def open(self, runtime_context):
        columns_path = "{}/feature_columns.json".format(MODELS_DIR)
        medians_path = "{}/feature_medians.json".format(MODELS_DIR)

        with open(columns_path, "r") as f:
            self.feature_columns = json.load(f)

        with open(medians_path, "r") as f:
            self.feature_medians = json.load(f)

        # Sanity check at startup rather than failing silently mid-stream.
        missing_medians = [c for c in self.feature_columns if c not in self.feature_medians]
        if missing_medians:
            logger.warning(
                "%d feature(s) have no saved median and will default to 0.0: %s",
                len(missing_medians),
                missing_medians[:10],
            )

    def map(self, raw_json):
        try:
            row = json.loads(raw_json)
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning("Dropping unparsable message: %s", e)
            return None

        # Join-key fields. cicflowmeter_producer.py normalizes CICFlowMeter's
        # "Src IP"/"Dst IP"/"Dst Port" columns into these lowercase keys
        # before publishing - see that script for the exact mapping.
        src_ip = row.get("src_ip")
        dst_ip = row.get("dst_ip")
        dst_port = row.get("dst_port")
        timestamp = row.get("timestamp")

        ordered_features = []
        for col in self.feature_columns:
            median = self.feature_medians.get(col, 0.0)
            ordered_features.append(safe_float(row.get(col), default=median))

        out = {
            "flow_key": flow_key(src_ip, dst_ip, dst_port),
            "src_ip": src_ip,
            "dst_ip": dst_ip,
            "dst_port": dst_port,
            "timestamp": timestamp,
            "feature_columns": self.feature_columns,
            "raw_features": ordered_features,
        }
        return json.dumps(out)


def build_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.enable_checkpointing(30000)  # 30s, so a TaskManager restart doesn't reprocess the world

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(TOPIC_RAW_FLOW_FEATURES)
        .set_group_id("feature-engineering-job")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(TOPIC_ENGINEERED_FEATURES)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    stream = env.from_source(
        source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="raw-flow-features-source",
    )

    engineered = (
        stream.map(FeatureEngineer(), output_type=Types.STRING())
        .filter(lambda x: x is not None)
        .name("feature-engineer")
    )

    engineered.sink_to(sink).name("engineered-features-sink")

    env.execute("nids-feature-engineering-job")


if __name__ == "__main__":
    build_pipeline()
