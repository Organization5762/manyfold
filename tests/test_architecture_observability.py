from __future__ import annotations

import json
import logging
import tempfile
import unittest
from pathlib import Path

from manyfold.architecture import (
    DEFAULT_LOG_BACKUP_COUNT,
    DEFAULT_LOG_MAX_BYTES,
    LogDestination,
    LogRecordEnvelope,
    MetricHistogramRecord,
    MetricsDestination,
    default_log_destination,
    default_metrics_destination,
)


class ArchitectureObservabilityTests(unittest.TestCase):
    def test_metrics_destination_publishes_otel_histogram_over_pubsub(self) -> None:
        destination = MetricsDestination(
            namespace="test.observability.metrics",
            buckets=(0.1, 1.0),
        )

        record = destination.record_histogram(
            "http.server.duration",
            0.2,
            unit="s",
            attributes={"route": "/healthz"},
            timestamp_ns=123,
        )
        latest = destination.topic.latest()

        self.assertIsInstance(record, MetricHistogramRecord)
        self.assertIsNotNone(latest)
        payload = json.loads(latest.payload)
        self.assertEqual(record.name, "http.server.duration")
        self.assertEqual(record.unit, "s")
        self.assertEqual(record.count, 1)
        self.assertEqual(record.sum, 0.2)
        self.assertEqual(json.loads(record.explicit_bounds_json), [0.1, 1.0])
        self.assertEqual(json.loads(record.bucket_counts_json), [0, 1, 0])
        self.assertEqual(json.loads(record.attributes_json), {"route": "/healthz"})
        self.assertEqual(payload["name"], "http.server.duration")
        self.assertIn("observability.metrics", destination.worker_handle.worker.roles)

    def test_log_destination_configures_rotating_file_and_collects_logs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manyfold.log"
            destination = LogDestination(
                namespace="test.observability.logs",
                path=path,
                max_bytes=1024,
                backup_count=2,
            )
            handler = destination.configure_root_logger(level=logging.INFO)
            try:
                logging.getLogger("manyfold.test").warning("collector ready")
                handler.flush()
            finally:
                logging.getLogger().removeHandler(handler)
                handler.close()

            lines = destination.query(limit=1)
            records = destination.collect_local_logs(limit=1)
            latest = destination.topic.latest()

        self.assertEqual(len(lines), 1)
        self.assertIn("collector ready", lines[0])
        self.assertEqual(len(records), 1)
        self.assertIsInstance(records[0], LogRecordEnvelope)
        self.assertIsNotNone(latest)
        payload = json.loads(latest.payload)
        self.assertIn("collector ready", records[0].body)
        self.assertEqual(records[0].severity_text, "INFO")
        self.assertIn("collector ready", payload["body"])
        self.assertIn("observability.logs", destination.worker_handle.worker.roles)

    def test_default_destinations_use_operational_bounds(self) -> None:
        metrics = default_metrics_destination(namespace="test.observability.defaults")

        with tempfile.TemporaryDirectory() as directory:
            logs = default_log_destination(
                namespace="test.observability.defaults",
                path=Path(directory) / "manyfold.log",
            )

        self.assertEqual(metrics.namespace, "test.observability.defaults")
        self.assertEqual(logs.max_bytes, DEFAULT_LOG_MAX_BYTES)
        self.assertEqual(logs.backup_count, DEFAULT_LOG_BACKUP_COUNT)

    def test_metrics_destination_rejects_unsorted_buckets(self) -> None:
        with self.assertRaisesRegex(ValueError, "buckets must be sorted"):
            MetricsDestination(buckets=(1.0, 0.1))


if __name__ == "__main__":
    unittest.main()
