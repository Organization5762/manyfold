"""Observability destinations backed by Manyfold PubSub infrastructure."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from logging.handlers import RotatingFileHandler
from pathlib import Path

from manyfold._manyfold_rust import (
    LogRecordEnvelope as LogRecordEnvelope,
    MetricHistogramRecord as MetricHistogramRecord,
    ObservabilityRuntime,
)

from .pubsub import PubSub, PubSubTopic
from .workers import WorkerEvent, WorkerHandle, WorkerRuntime

DEFAULT_LOG_BACKUP_COUNT = 5
DEFAULT_LOG_FILE = Path(".manyfold") / "logs" / "manyfold.log"
DEFAULT_LOG_MAX_BYTES = 100 * 1024 * 1024
DEFAULT_LOG_TOPIC = "logs"
DEFAULT_METRIC_BUCKETS = (0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0)
DEFAULT_METRICS_TOPIC = "metrics.histograms"
DEFAULT_OBSERVABILITY_NAMESPACE = "manyfold.observability"
DEFAULT_WORKER_TOPIC = "workers"


def default_log_destination(
    *,
    namespace: str = DEFAULT_OBSERVABILITY_NAMESPACE,
    path: str | Path = DEFAULT_LOG_FILE,
    max_bytes: int = DEFAULT_LOG_MAX_BYTES,
    backup_count: int = DEFAULT_LOG_BACKUP_COUNT,
) -> "LogDestination":
    """Return the default rotating-file log destination and PubSub collector."""
    return LogDestination(
        namespace=namespace,
        path=path,
        max_bytes=max_bytes,
        backup_count=backup_count,
    )


def default_metrics_destination(
    *,
    namespace: str = DEFAULT_OBSERVABILITY_NAMESPACE,
    buckets: Sequence[float] = DEFAULT_METRIC_BUCKETS,
) -> "MetricsDestination":
    """Return the default OTEL-compatible metrics destination."""
    return MetricsDestination(namespace=namespace, buckets=buckets)


class LogDestination:
    """Rotating local log destination with PubSub collection."""

    def __init__(
        self,
        *,
        namespace: str = DEFAULT_OBSERVABILITY_NAMESPACE,
        path: str | Path = DEFAULT_LOG_FILE,
        max_bytes: int = DEFAULT_LOG_MAX_BYTES,
        backup_count: int = DEFAULT_LOG_BACKUP_COUNT,
        topic: PubSub | None = None,
        worker_topic: PubSub | None = None,
        runtime: ObservabilityRuntime | None = None,
    ) -> None:
        self.namespace = _require_text(namespace, "namespace")
        self.path = Path(path)
        self.max_bytes = _require_positive_int(max_bytes, "max_bytes")
        self.backup_count = _require_nonnegative_int(backup_count, "backup_count")
        self.topic = topic or PubSubTopic(
            DEFAULT_LOG_TOPIC,
            namespace=self.namespace,
        )
        self._runtime = runtime or ObservabilityRuntime()
        self.worker_handle = _attach_observability_worker(
            worker_topic,
            namespace=self.namespace,
            name=f"{self.namespace}.logs",
            role="observability.logs",
        )

    def configure_root_logger(
        self,
        *,
        level: int = logging.INFO,
        formatter: logging.Formatter | None = None,
    ) -> RotatingFileHandler:
        """Install a rotating local file handler on the root logger."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(
            self.path,
            maxBytes=self.max_bytes,
            backupCount=self.backup_count,
            encoding="utf-8",
        )
        handler.setFormatter(formatter or _default_log_formatter())
        root = logging.getLogger()
        root.setLevel(level)
        root.addHandler(handler)
        return handler

    def collect_local_logs(self, *, limit: int = 100) -> tuple[LogRecordEnvelope, ...]:
        """Read local log lines and publish them to the PubSub log topic."""
        records = tuple(self._record_from_line(line) for line in self.query(limit=limit))
        for record in records:
            self.topic.publish(_record_payload(record))
        return records

    def query(self, *, limit: int = 100) -> tuple[str, ...]:
        """Return recent log lines from the rotating local log files."""
        limit = _require_positive_int(limit, "limit")
        lines: list[str] = []
        for path in reversed(self._log_paths()):
            if not path.exists():
                continue
            file_lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
            lines.extend(file_lines)
            if len(lines) >= limit:
                break
        return tuple(lines[-limit:])

    def _log_paths(self) -> tuple[Path, ...]:
        backups = tuple(
            self.path.with_name(f"{self.path.name}.{index}")
            for index in range(self.backup_count, 0, -1)
        )
        return (*backups, self.path)

    def _record_from_line(self, line: str) -> LogRecordEnvelope:
        return self._runtime.record_log(
            "INFO",
            "manyfold.local_file",
            line,
            file_path=str(self.path),
            line_number=0,
            attributes_json=_attributes_json({"source": "local_file"}),
        )


class MetricsDestination:
    """Histogram metrics destination backed by a PubSub sidecar topic."""

    def __init__(
        self,
        *,
        namespace: str = DEFAULT_OBSERVABILITY_NAMESPACE,
        buckets: Sequence[float] = DEFAULT_METRIC_BUCKETS,
        topic: PubSub | None = None,
        worker_topic: PubSub | None = None,
        runtime: ObservabilityRuntime | None = None,
    ) -> None:
        self.namespace = _require_text(namespace, "namespace")
        self.buckets = _validate_buckets(buckets)
        self.topic = topic or PubSubTopic(
            DEFAULT_METRICS_TOPIC,
            namespace=self.namespace,
        )
        self._runtime = runtime or ObservabilityRuntime()
        self._runtime.set_buckets(list(self.buckets))
        self.worker_handle = _attach_observability_worker(
            worker_topic,
            namespace=self.namespace,
            name=f"{self.namespace}.metrics",
            role="observability.metrics",
        )

    def record_histogram(
        self,
        name: str,
        value: float,
        *,
        unit: str = "",
        attributes: Mapping[str, object] | None = None,
        timestamp_ns: int | None = None,
    ) -> MetricHistogramRecord:
        """Publish one OTEL-compatible histogram sample."""
        value = _require_number(value, "value")
        record = self._runtime.record_histogram(
            _require_text(name, "metric name"),
            value,
            unit=unit,
            attributes_json=_attributes_json(attributes or {}),
            timestamp_ns=timestamp_ns,
        )
        self.topic.publish(_record_payload(record))
        return record


def _attach_observability_worker(
    worker_topic: PubSub | None,
    *,
    namespace: str,
    name: str,
    role: str,
) -> WorkerHandle:
    topic = worker_topic or PubSubTopic(
        DEFAULT_WORKER_TOPIC,
        namespace=namespace,
        schema=WorkerEvent,
    )
    return WorkerRuntime(pubsub=topic).attach_current_thread(
        name,
        roles=(role, "observability.sidecar"),
    )


def _attributes_json(attributes: Mapping[str, object]) -> str:
    return json.dumps(dict(attributes), sort_keys=True, separators=(",", ":"))


def _default_log_formatter() -> logging.Formatter:
    return logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(process)d %(thread)d %(message)s"
    )


def _require_nonnegative_int(value: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} must be an integer")
    if value < 0:
        raise ValueError(f"{field} must not be negative")
    return value


def _require_number(value: float, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{field} must be a number")
    return float(value)


def _require_positive_int(value: int, field: str) -> int:
    value = _require_nonnegative_int(value, field)
    if value == 0:
        raise ValueError(f"{field} must be positive")
    return value


def _require_text(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


def _record_payload(record: object) -> bytes:
    fields = {
        name: getattr(record, name)
        for name in dir(record)
        if not name.startswith("_") and not callable(getattr(record, name))
    }
    return json.dumps(fields, sort_keys=True, separators=(",", ":")).encode()


def _validate_buckets(buckets: Sequence[float]) -> tuple[float, ...]:
    resolved = tuple(_require_number(bucket, "bucket") for bucket in buckets)
    if not resolved:
        raise ValueError("buckets must not be empty")
    if tuple(sorted(resolved)) != resolved:
        raise ValueError("buckets must be sorted")
    return resolved


__all__ = [
    "DEFAULT_LOG_BACKUP_COUNT",
    "DEFAULT_LOG_FILE",
    "DEFAULT_LOG_MAX_BYTES",
    "DEFAULT_LOG_TOPIC",
    "DEFAULT_METRICS_TOPIC",
    "DEFAULT_METRIC_BUCKETS",
    "DEFAULT_OBSERVABILITY_NAMESPACE",
    "DEFAULT_WORKER_TOPIC",
    "LogDestination",
    "LogRecordEnvelope",
    "MetricHistogramRecord",
    "MetricsDestination",
    "default_log_destination",
    "default_metrics_destination",
]
