"""Topic retention policies for durable transport delivery."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path

DEFAULT_DELIVERY_ITEM_LIMIT = 1024
DEFAULT_DELIVERY_MAX_ATTEMPTS = 64
DEFAULT_DELIVERY_SOFT_LIMIT_RATIO = 0.7
DEFAULT_DELIVERY_STORAGE_BYTES = 64 * 1024 * 1024
DEFAULT_RENDERED_FRAME_TTL_SECONDS = 0.2
MAX_FRAME_TICK_TTL_SECONDS = 0.05


class DeliverySemantics(str, Enum):
    """Persistence behavior for one outbound topic."""

    APPEND = "append"
    LATEST = "latest"


@dataclass(frozen=True, slots=True)
class TopicDeliveryPolicy:
    """Per-topic journal bounds and append-versus-latest semantics."""

    topic: str
    semantics: DeliverySemantics
    max_items: int
    max_bytes: int
    ttl_seconds: float
    max_attempts: int
    soft_limit_ratio: float = DEFAULT_DELIVERY_SOFT_LIMIT_RATIO
    latest_per_source: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "topic", _require_text(self.topic, "topic"))
        if not isinstance(self.semantics, DeliverySemantics):
            raise ValueError("semantics must be a DeliverySemantics")
        _require_positive_integer(self.max_items, "max_items")
        _require_positive_integer(self.max_bytes, "max_bytes")
        _require_positive_number(self.ttl_seconds, "ttl_seconds")
        _require_positive_integer(self.max_attempts, "max_attempts")
        _require_ratio(self.soft_limit_ratio, "soft_limit_ratio")
        if not isinstance(self.latest_per_source, bool):
            raise ValueError("latest_per_source must be a boolean")
        if (
            self.semantics is DeliverySemantics.APPEND
            and self.latest_per_source
        ):
            raise ValueError("append policies cannot be latest_per_source")

    @classmethod
    def commands(
        cls,
        topic: str,
        *,
        max_items: int,
        max_bytes: int,
        ttl_seconds: float = 24 * 60 * 60,
        max_attempts: int = DEFAULT_DELIVERY_MAX_ATTEMPTS,
        soft_limit_ratio: float = DEFAULT_DELIVERY_SOFT_LIMIT_RATIO,
    ) -> "TopicDeliveryPolicy":
        """Build a bounded append/deduplicated command policy."""
        return cls(
            topic,
            DeliverySemantics.APPEND,
            max_items,
            max_bytes,
            ttl_seconds,
            max_attempts,
            soft_limit_ratio,
        )

    @classmethod
    def latest(
        cls,
        topic: str,
        *,
        max_sources: int,
        max_bytes: int,
        ttl_seconds: float,
        max_attempts: int = 4,
        soft_limit_ratio: float = 0.5,
    ) -> "TopicDeliveryPolicy":
        """Build a short-lived latest-value slot per named source."""
        return cls(
            topic,
            DeliverySemantics.LATEST,
            max_sources,
            max_bytes,
            ttl_seconds,
            max_attempts,
            soft_limit_ratio,
            latest_per_source=True,
        )

    @classmethod
    def frame_ticks(
        cls,
        topic: str,
        *,
        max_bytes: int,
        cadence_seconds: float = MAX_FRAME_TICK_TTL_SECONDS,
    ) -> "TopicDeliveryPolicy":
        """Build one non-backlogging frame-tick slot."""
        cadence_seconds = _require_positive_number(
            cadence_seconds,
            "cadence_seconds",
        )
        return cls(
            topic,
            DeliverySemantics.LATEST,
            1,
            max_bytes,
            min(cadence_seconds, MAX_FRAME_TICK_TTL_SECONDS),
            1,
            0.5,
        )

    @classmethod
    def rendered_frames(
        cls,
        topic: str,
        *,
        max_sources: int,
        max_bytes: int,
        ttl_seconds: float = DEFAULT_RENDERED_FRAME_TTL_SECONDS,
    ) -> "TopicDeliveryPolicy":
        """Build latest rendered-frame slots with a 200 ms default TTL."""
        return cls.latest(
            topic,
            max_sources=max_sources,
            max_bytes=max_bytes,
            ttl_seconds=ttl_seconds,
            max_attempts=2,
            soft_limit_ratio=0.5,
        )


@dataclass(frozen=True, slots=True)
class DeliveryConfig:
    """Durability, retry, expiry, and memory limits for one delivery endpoint."""

    journal_path: Path
    max_outbox_items: int = DEFAULT_DELIVERY_ITEM_LIMIT
    max_inbox_items: int = DEFAULT_DELIVERY_ITEM_LIMIT
    max_storage_bytes: int = DEFAULT_DELIVERY_STORAGE_BYTES
    receive_queue_limit: int = 256
    max_message_bytes: int = 8 * 1024 * 1024
    message_ttl_seconds: float = 24 * 60 * 60
    dedupe_retention_seconds: float = 24 * 60 * 60
    retry_initial_seconds: float = 0.1
    retry_multiplier: float = 2.0
    retry_max_seconds: float = 5.0
    max_delivery_attempts: int = DEFAULT_DELIVERY_MAX_ATTEMPTS
    soft_limit_ratio: float = DEFAULT_DELIVERY_SOFT_LIMIT_RATIO
    topic_policies: tuple[TopicDeliveryPolicy, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.journal_path, Path):
            raise ValueError("journal_path must be a pathlib.Path")
        _require_positive_integer(self.max_outbox_items, "max_outbox_items")
        _require_positive_integer(self.max_inbox_items, "max_inbox_items")
        _require_positive_integer(self.max_storage_bytes, "max_storage_bytes")
        _require_positive_integer(self.receive_queue_limit, "receive_queue_limit")
        _require_positive_integer(self.max_message_bytes, "max_message_bytes")
        _require_positive_number(self.message_ttl_seconds, "message_ttl_seconds")
        _require_positive_number(
            self.dedupe_retention_seconds,
            "dedupe_retention_seconds",
        )
        _require_positive_number(
            self.retry_initial_seconds,
            "retry_initial_seconds",
        )
        _require_positive_number(self.retry_multiplier, "retry_multiplier")
        _require_positive_number(self.retry_max_seconds, "retry_max_seconds")
        _require_positive_integer(
            self.max_delivery_attempts,
            "max_delivery_attempts",
        )
        _require_ratio(self.soft_limit_ratio, "soft_limit_ratio")
        if not isinstance(self.topic_policies, tuple) or not all(
            isinstance(policy, TopicDeliveryPolicy)
            for policy in self.topic_policies
        ):
            raise ValueError(
                "topic_policies must be a tuple of TopicDeliveryPolicy values"
            )
        topics = tuple(policy.topic for policy in self.topic_policies)
        if len(topics) != len(set(topics)):
            raise ValueError("topic_policies must contain unique topics")
        if self.max_storage_bytes < 64 * 1024:
            raise ValueError("max_storage_bytes must be at least 65536")
        if self.retry_multiplier < 1:
            raise ValueError("retry_multiplier must be at least 1")
        if self.retry_max_seconds < self.retry_initial_seconds:
            raise ValueError(
                "retry_max_seconds must be at least retry_initial_seconds"
            )


def _require_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _require_positive_integer(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")


def _require_positive_number(value: float, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float) or value <= 0:
        raise ValueError(f"{field_name} must be a positive number")
    return float(value)


def _require_ratio(value: float, field_name: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not 0 < value < 1
    ):
        raise ValueError(f"{field_name} must be greater than 0 and less than 1")
    return float(value)


def _require_optional_timeout(value: float | None) -> None:
    if value is not None:
        _require_nonnegative_number(value, "timeout")


def _require_nonnegative_number(value: float, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative number")


__all__ = [
    "DEFAULT_DELIVERY_ITEM_LIMIT",
    "DEFAULT_DELIVERY_MAX_ATTEMPTS",
    "DEFAULT_DELIVERY_SOFT_LIMIT_RATIO",
    "DEFAULT_DELIVERY_STORAGE_BYTES",
    "DEFAULT_RENDERED_FRAME_TTL_SECONDS",
    "DeliveryConfig",
    "DeliverySemantics",
    "MAX_FRAME_TICK_TTL_SECONDS",
    "TopicDeliveryPolicy",
]
