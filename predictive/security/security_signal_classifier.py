"""
OmniWatch — Predictive Intelligence Layer
Component: Security Signal Classifier (GAP 1 Main)
Phase: 6
Purpose: Consume security events from Kafka, route to sub-detectors, produce
         SecurityAnomalySignal to omniwatch.anomalies.detected
Inputs: Security event dicts from omniwatch.security.events Kafka topic
Outputs: SecurityAnomalySignal dicts to omniwatch.anomalies.detected topic
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, Optional

from .brute_force_detector import BruteForceDetector
from .config_drift_detector import ConfigDriftDetector
from .data_exfil_detector import DataExfilDetector
from .evidence_aggregator import EvidenceAggregator
from .priv_escalation_detector import PrivEscalationDetector

logger = logging.getLogger("omniwatch.predictive.security.classifier")

# Canonical Kafka topics (mirrors ingestion/kafka_bus.py)
TOPIC_SECURITY_EVENTS = "omniwatch.security.events"
TOPIC_ANOMALIES_DETECTED = "omniwatch.anomalies.detected"

# Consumer group for the security signal classifier
CONSUMER_GROUP = "omniwatch-predictive-security"


class SecuritySignalClassifier:
    """Main security signal classifier that consumes from Kafka, routes events
    to sub-detectors, and produces SecurityAnomalySignal to the anomalies topic.

    This is the GAP 1 implementation — a dedicated security anomaly detection
    pipeline separate from the performance anomaly detection pipeline.

    Usage::

        classifier = SecuritySignalClassifier()
        classifier.start()
        classifier.run()  # Blocks, consuming from Kafka
        classifier.stop()
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        *,
        topic_in: str = TOPIC_SECURITY_EVENTS,
        topic_out: str = TOPIC_ANOMALIES_DETECTED,
        consumer_group: str = CONSUMER_GROUP,
    ) -> None:
        """Initialise the classifier with Kafka connection parameters.

        Parameters
        ----------
        bootstrap_servers : str
            Kafka broker addresses.
        topic_in : str
            Kafka topic to consume security events from.
        topic_out : str
            Kafka topic to publish SecurityAnomalySignal to.
        consumer_group : str
            Kafka consumer group ID.
        """
        self._bootstrap_servers = bootstrap_servers
        self._topic_in = topic_in
        self._topic_out = topic_out
        self._consumer_group = consumer_group

        # Sub-detectors
        self._brute_force = BruteForceDetector()
        self._config_drift = ConfigDriftDetector()
        self._priv_escalation = PrivEscalationDetector()
        self._data_exfil = DataExfilDetector()

        # Evidence aggregator
        self._evidence = EvidenceAggregator()

        # Kafka components (lazy — created in start())
        self._consumer = None
        self._producer = None
        self._running = False

        logger.info(
            "SecuritySignalClassifier initialised — consumer_group=%s "
            "topic_in=%s topic_out=%s",
            self._consumer_group,
            self._topic_in,
            self._topic_out,
        )

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def start(self) -> None:
        """Initialise Kafka consumer and producer connections."""
        from kafka import KafkaConsumer as _KafkaConsumer
        from kafka import KafkaProducer as _KafkaProducer

        self._consumer = _KafkaConsumer(
            self._topic_in,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._consumer_group,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

        self._producer = _KafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            acks="all",
            retries=3,
            retry_backoff_ms=500,
        )

        self._running = True
        logger.info(
            "SecuritySignalClassifier started — consuming from %s",
            self._topic_in,
        )

    def stop(self) -> None:
        """Gracefully shutdown Kafka consumer and producer."""
        self._running = False
        if self._consumer:
            self._consumer.close()
            self._consumer = None
        if self._producer:
            self._producer.flush(timeout=5.0)
            self._producer.close()
            self._producer = None
        logger.info("SecuritySignalClassifier stopped")

    def run(self) -> None:
        """Main consumption loop — blocks until stop() is called."""
        if not self._running:
            self.start()

        logger.info("SecuritySignalClassifier entering main loop")

        try:
            while self._running:
                if self._consumer is None:
                    break
                # Poll for messages with a 1-second timeout
                records = self._consumer.poll(timeout_ms=1000)
                for topic_partition, messages in records.items():
                    for message in messages:
                        try:
                            self._process_event(message.value)
                        except Exception:
                            logger.warning(
                                "Failed to process security event",
                                exc_info=True,
                            )
        except KeyboardInterrupt:
            logger.info("SecuritySignalClassifier interrupted")
        finally:
            self.stop()

    def process_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single security event and return the signal (if any).

        This is the public entry point for testing — it doesn't require
        Kafka infrastructure.

        Parameters
        ----------
        event : dict
            A security event dict from the ``omniwatch.security.events`` topic.

        Returns
        -------
        dict | None
            A ``SecurityAnomalySignal`` dict if an anomaly is detected,
            ``None`` otherwise.
        """
        return self._process_event(event)

    # ------------------------------------------------------------------ #
    # Internal routing
    # ------------------------------------------------------------------ #

    def _process_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Route event to appropriate sub-detector and publish signal."""
        if not isinstance(event, dict):
            logger.debug("SecuritySignalClassifier: non-dict event ignored")
            return None

        # Collect evidence for this event
        evidence_lines = self._evidence.collect(event)

        # Route based on attack_type or event_type
        signal = self._route_to_detector(event)

        if signal is None:
            return None

        # Ensure source_type is "security"
        signal["source_type"] = "security"

        # Attach aggregated evidence logs — preserve sub-detector evidence
        # as fallback when aggregator has no data for this event
        has_real_evidence = any(line.strip() for line in evidence_lines)
        if has_real_evidence:
            signal["evidence_logs"] = evidence_lines
        # else: keep the evidence_logs from the sub-detector

        # Publish to Kafka
        self._publish(signal)

        return signal

    def _route_to_detector(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Route event to the appropriate sub-detector based on event type."""
        # Extract attack_type / event_type for routing
        attack_type = str(event.get("attack_type", "")).lower()
        event_type = str(event.get("event_type", "")).lower()
        combined_type = f"{attack_type} {event_type}"

        # Check for config drift patterns first (unauthorized config change)
        if self._is_config_drift(combined_type, event):
            return self._config_drift.detect(event)

        # Check for brute force patterns
        if self._is_brute_force(combined_type, event):
            return self._brute_force.detect(event)

        # Check for privilege escalation patterns
        if self._is_priv_escalation(combined_type, event):
            return self._priv_escalation.detect(event)

        # Check for data exfiltration (outbound_bytes metric)
        if self._is_data_exfil(event):
            return self._data_exfil.detect(event)

        # Unknown event type — try all detectors as fallback
        signal = self._brute_force.detect(event)
        if signal:
            return signal

        signal = self._config_drift.detect(event)
        if signal:
            return signal

        signal = self._priv_escalation.detect(event)
        if signal:
            return signal

        signal = self._data_exfil.detect(event)
        if signal:
            return signal

        logger.debug(
            "SecuritySignalClassifier: no detector matched event type=%s",
            event.get("attack_type", event.get("event_type", "unknown")),
        )
        return None

    def _is_config_drift(self, combined_type: str, event: Dict[str, Any]) -> bool:
        """Check if event is a config drift type."""
        config_keywords = [
            "config_drift",
            "config_file_changed",
            "unauthorized_config",
            "unauthorized.*config",
        ]
        return any(kw in combined_type for kw in config_keywords)

    def _is_brute_force(self, combined_type: str, event: Dict[str, Any]) -> bool:
        """Check if event is a brute force type."""
        brute_keywords = ["brute_force", "auth_failure", "failed_login", "login_fail"]
        return any(kw in combined_type for kw in brute_keywords)

    def _is_priv_escalation(self, combined_type: str, event: Dict[str, Any]) -> bool:
        """Check if event is a privilege escalation type."""
        priv_keywords = ["privilege_escalation", "priv_escalation", "sudo", "role_change"]
        return any(kw in combined_type for kw in priv_keywords)

    def _is_data_exfil(self, event: Dict[str, Any]) -> bool:
        """Check if event is a data exfiltration type."""
        # Data exfiltration is detected by outbound_bytes metric presence
        return "outbound_bytes" in event

    def _publish(self, signal: Dict[str, Any]) -> None:
        """Publish a SecurityAnomalySignal to Kafka."""
        if self._producer is None:
            logger.debug(
                "SecuritySignalClassifier: producer not initialised — "
                "signal not published"
            )
            return

        try:
            self._producer.send(self._topic_out, signal)
            logger.debug(
                "Published security anomaly — entity=%s attack_type=%s",
                signal.get("entity_id"),
                signal.get("attack_type"),
            )
        except Exception:
            logger.warning(
                "Failed to publish security anomaly to Kafka",
                exc_info=True,
            )
