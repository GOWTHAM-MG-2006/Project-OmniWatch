"""OmniWatch — Causal Graph Engine
Component: seed_anomalies
Phase: 7
Purpose: Seed omniwatch.anomalies.detected with a valid AnomalySignal so the
         causal engine can consume and publish a RootCauseObject. This is a
         test harness for the blocked upstream entity-resolution pipeline.
Inputs: none
Outputs: omniwatch.anomalies.detected (Kafka)
"""
import json
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "omniwatch.anomalies.detected"
BOOTSTRAP = "kafka:29092"


def build_signal(entity_id: str, metric: str, score: float) -> dict:
    return {
        "entity_id": entity_id,
        "entity_type": "DATABASE_NODE",
        "metric_name": metric,
        "anomaly_score": score,
        "confidence": 92.0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "deviation_from_baseline": 4.2,
        "source_type": "performance",
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    signals = [
        build_signal("postgresql-database", "query_latency_p99", 0.95),
        build_signal("background-worker", "memory_usage", 0.88),
    ]
    for sig in signals:
        producer.send(TOPIC, value=sig)
        print(f"seeded {sig['entity_id']} -> {TOPIC}")
    producer.flush()
    producer.close()
    print("done")


if __name__ == "__main__":
    main()