"""
Console consumer for the standalone ML engine.
==============================================

Prints, as one pretty JSON object per line, whatever the ML engine writes
(or reads). Handy for eyeballing outputs without the Kafka UI.

    python ml-engine\\tools\\consume_topic.py --topic nids-alerts
    python ml-engine\\tools\\consume_topic.py --topic engineered-features --max-messages 10
    python ml-engine\\tools\\consume_topic.py --topic raw-flow-features   # what you fed in

Uses a throwaway consumer group and 'latest' offset by default, so it never
disturbs the FLINK jobs' own consumer groups.
"""

import argparse
import json
import uuid

from kafka import KafkaConsumer

TOPICS = ["raw-flow-features", "suricata-alerts", "engineered-features", "nids-alerts"]


def main():
    parser = argparse.ArgumentParser(description="Consume and print messages from an ML-engine topic.")
    parser.add_argument("--topic", required=True, choices=TOPICS)
    parser.add_argument("--bootstrap-servers", default="localhost:29092")
    parser.add_argument("--max-messages", type=int, default=0, help="0 = run forever")
    parser.add_argument("--from-beginning", action="store_true",
                        help="Replay from the earliest offset instead of only new messages")
    parser.add_argument("--raw", action="store_true", help="Print raw strings instead of pretty JSON")
    args = parser.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id="ml-engine-console-" + uuid.uuid4().hex[:8],
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        enable_auto_commit=False,
        value_deserializer=lambda v: v.decode("utf-8", errors="replace"),
    )

    print("Consuming '{}' from {} (Ctrl+C to stop)...".format(args.topic, args.bootstrap_servers))
    count = 0
    try:
        for message in consumer:
            value = message.value
            if not args.raw:
                try:
                    value = json.dumps(json.loads(value), indent=2, ensure_ascii=False)
                except (ValueError, TypeError):
                    pass
            print(value)
            count += 1
            if args.max_messages and count >= args.max_messages:
                break
    except KeyboardInterrupt:
        print("\nStopped after {} message(s).".format(count))
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
