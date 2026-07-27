"""Configuration and topic policy for durable transport delivery."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from math import exp, isfinite, log
from pathlib import Path
from typing import final

from ._transport_delivery_protocol import _MAX_DELIVERY_ATTEMPT

DEFAULT_DELIVERY_ITEM_LIMIT = 1024
DEFAULT_DELIVERY_MAX_ATTEMPTS = 64
DEFAULT_DELIVERY_RECOVERY_BATCH_SIZE = 64
DEFAULT_DELIVERY_SOFT_LIMIT_RATIO = 0.7
DEFAULT_DELIVERY_STORAGE_BYTES = 64 * 1024 * 1024
DEFAULT_DELIVERY_WORK_BATCH_SIZE = 32

_MAX_LOCAL_PRESSURE_EXPONENT = 16
_MAX_DELIVERY_STORAGE_BYTES = DEFAULT_DELIVERY_STORAGE_BYTES + 0
_VOLATILE_TOPIC_TOKENS_V1 = frozenset(
    {"audio", "debug", "frame", "input", "microphone", "render", "tick"}
)
_CHANNEL_TOKEN_PATTERN = re.compile(r"[^\W_]+")


@final
class DeliverySemantics(str, Enum):
    """Persistence behavior for one explicitly configured durable topic."""

    APPEND = "append"
    LATEST = "latest"


@final
@dataclass(frozen=True, slots=True)
class TopicDeliveryPolicy:
    """Hard bounds for a durable command or source-keyed latest-value topic."""

    topic: str
    semantics: DeliverySemantics
    max_items: int
    max_bytes: int
    ttl_seconds: float
    max_attempts: int = DEFAULT_DELIVERY_MAX_ATTEMPTS
    soft_limit_ratio: float = DEFAULT_DELIVERY_SOFT_LIMIT_RATIO
    max_inbox_items: int | None = None
    max_inbox_bytes: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "topic", _require_durable_topic(self.topic))
        if not isinstance(self.semantics, DeliverySemantics):
            raise ValueError("semantics must be a DeliverySemantics")
        _require_positive_integer(self.max_items, "max_items")
        _require_positive_integer(self.max_bytes, "max_bytes")
        _require_positive_number(self.ttl_seconds, "ttl_seconds")
        _require_positive_integer(self.max_attempts, "max_attempts")
        if self.max_attempts > _MAX_DELIVERY_ATTEMPT:
            raise ValueError(
                "max_attempts exceeds the delivery protocol uint32 limit"
            )
        _require_ratio(self.soft_limit_ratio, "soft_limit_ratio")
        inbox_items = (
            self.max_items
            if self.max_inbox_items is None
            else self.max_inbox_items
        )
        inbox_bytes = (
            self.max_bytes
            if self.max_inbox_bytes is None
            else self.max_inbox_bytes
        )
        _require_positive_integer(inbox_items, "max_inbox_items")
        _require_positive_integer(inbox_bytes, "max_inbox_bytes")
        object.__setattr__(self, "max_inbox_items", inbox_items)
        object.__setattr__(self, "max_inbox_bytes", inbox_bytes)

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
        max_inbox_items: int | None = None,
        max_inbox_bytes: int | None = None,
    ) -> TopicDeliveryPolicy:
        """Build bounded append-and-deduplicate command semantics."""
        return cls(
            topic,
            DeliverySemantics.APPEND,
            max_items,
            max_bytes,
            ttl_seconds,
            max_attempts,
            soft_limit_ratio,
            max_inbox_items,
            max_inbox_bytes,
        )

    @classmethod
    def latest(
        cls,
        topic: str,
        *,
        max_sources: int,
        max_bytes: int,
        ttl_seconds: float,
        max_attempts: int = DEFAULT_DELIVERY_MAX_ATTEMPTS,
        soft_limit_ratio: float = DEFAULT_DELIVERY_SOFT_LIMIT_RATIO,
        max_inbox_items: int = DEFAULT_DELIVERY_ITEM_LIMIT,
        max_inbox_bytes: int | None = None,
    ) -> TopicDeliveryPolicy:
        """Build one short-lived durable latest-value slot per source."""
        return cls(
            topic,
            DeliverySemantics.LATEST,
            max_sources,
            max_bytes,
            ttl_seconds,
            max_attempts,
            soft_limit_ratio,
            max_inbox_items,
            max_bytes if max_inbox_bytes is None else max_inbox_bytes,
        )


@final
@dataclass(frozen=True, slots=True)
class DeliveryConfig:
    """Durability, retry, expiry, recovery, and memory limits for one peer."""

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
    work_batch_size: int = DEFAULT_DELIVERY_WORK_BATCH_SIZE
    recovery_batch_size: int = DEFAULT_DELIVERY_RECOVERY_BATCH_SIZE
    max_delivery_attempts: int = DEFAULT_DELIVERY_MAX_ATTEMPTS
    max_ack_attempts: int = DEFAULT_DELIVERY_MAX_ATTEMPTS
    local_pressure_exponent_limit: int = 16
    soft_limit_ratio: float = DEFAULT_DELIVERY_SOFT_LIMIT_RATIO
    worker_join_timeout_seconds: float = 2.0
    topic_policies: tuple[TopicDeliveryPolicy, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.journal_path, Path):
            raise ValueError("journal_path must be a pathlib.Path")
        if str(self.journal_path) == ":memory:":
            raise ValueError(
                "journal_path must be a durable filesystem path, not :memory:"
            )
        canonical_path = self.journal_path.resolve()
        if canonical_path.exists() and not canonical_path.is_file():
            raise ValueError(
                "journal_path must identify a regular file, not a "
                "directory or special target"
            )
        if canonical_path.exists() and canonical_path.stat().st_nlink > 1:
            raise ValueError(
                "journal_path must not be a hard-linked file"
            )
        object.__setattr__(self, "journal_path", canonical_path)
        for name in (
            "max_outbox_items",
            "max_inbox_items",
            "max_storage_bytes",
            "receive_queue_limit",
            "work_batch_size",
            "recovery_batch_size",
            "max_message_bytes",
            "max_delivery_attempts",
            "max_ack_attempts",
            "local_pressure_exponent_limit",
        ):
            _require_positive_integer(getattr(self, name), name)
        for name in (
            "message_ttl_seconds",
            "dedupe_retention_seconds",
            "retry_initial_seconds",
            "retry_multiplier",
            "retry_max_seconds",
            "worker_join_timeout_seconds",
        ):
            _require_positive_number(getattr(self, name), name)
        _require_ratio(self.soft_limit_ratio, "soft_limit_ratio")
        if self.max_delivery_attempts > _MAX_DELIVERY_ATTEMPT:
            raise ValueError(
                "max_delivery_attempts exceeds the delivery protocol "
                "uint32 limit"
            )
        if self.max_ack_attempts > _MAX_DELIVERY_ATTEMPT:
            raise ValueError(
                "max_ack_attempts exceeds the delivery persisted "
                "uint32 limit"
            )
        if self.local_pressure_exponent_limit > _MAX_LOCAL_PRESSURE_EXPONENT:
            raise ValueError(
                "local_pressure_exponent_limit cannot exceed 16"
            )
        if self.max_storage_bytes < 64 * 1024:
            raise ValueError("max_storage_bytes must be at least 65536")
        if self.max_storage_bytes > _MAX_DELIVERY_STORAGE_BYTES:
            raise ValueError(
                "max_storage_bytes cannot exceed 67108864"
            )
        if self.retry_multiplier < 1:
            raise ValueError("retry_multiplier must be at least 1")
        if self.retry_max_seconds < self.retry_initial_seconds:
            raise ValueError(
                "retry_max_seconds must be at least retry_initial_seconds"
            )
        if self.recovery_batch_size > min(
            self.max_outbox_items,
            self.max_inbox_items,
        ):
            raise ValueError(
                "recovery_batch_size cannot exceed either retained item limit"
            )
        if not isinstance(self.topic_policies, tuple):
            raise ValueError("topic_policies must be a tuple")
        topics: set[str] = set()
        for policy in self.topic_policies:
            if not isinstance(policy, TopicDeliveryPolicy):
                raise ValueError(
                    "topic_policies must contain TopicDeliveryPolicy values"
                )
            if policy.topic in topics:
                raise ValueError(
                    f"topic_policies contains duplicate topic {policy.topic!r}"
                )
            if policy.ttl_seconds > self.message_ttl_seconds:
                raise ValueError(
                    f"topic {policy.topic!r} ttl_seconds exceeds "
                    "message_ttl_seconds"
                )
            if policy.max_attempts > self.max_delivery_attempts:
                raise ValueError(
                    f"topic {policy.topic!r} max_attempts exceeds "
                    "max_delivery_attempts"
                )
            if policy.max_items > self.max_outbox_items:
                raise ValueError(
                    f"topic {policy.topic!r} max_items exceeds "
                    "max_outbox_items"
                )
            if int(policy.max_inbox_items) > self.max_inbox_items:
                raise ValueError(
                    f"topic {policy.topic!r} max_inbox_items exceeds "
                    "max_inbox_items"
                )
            if policy.max_bytes > self.max_storage_bytes:
                raise ValueError(
                    f"topic {policy.topic!r} max_bytes exceeds "
                    "max_storage_bytes"
                )
            if int(policy.max_inbox_bytes) > self.max_storage_bytes:
                raise ValueError(
                    f"topic {policy.topic!r} max_inbox_bytes exceeds "
                    "max_storage_bytes"
                )
            topics.add(policy.topic)

    def policy_for(self, topic: str) -> TopicDeliveryPolicy:
        """Return the explicit policy for ``topic`` or reject durable use."""
        normalized = _require_durable_topic(topic)
        for policy in self.topic_policies:
            if policy.topic == normalized:
                return policy
        raise ValueError(
            f"durable topic {normalized!r} has no explicit TopicDeliveryPolicy"
        )


def _require_durable_topic(value: str) -> str:
    topic = _require_text(value, "topic")
    if _is_volatile_delivery_topic(topic):
        raise ValueError(
            f"topic {topic!r} contains a volatile V1 channel token and "
            "cannot be durable"
        )
    return topic


def _is_volatile_delivery_topic(topic: str) -> bool:
    """Classify V1 hot paths by exact, case-folded separator-delimited tokens."""
    return bool(
        _VOLATILE_TOPIC_TOKENS_V1.intersection(
            token.casefold() for token in _CHANNEL_TOKEN_PATTERN.findall(topic)
        )
    )


def _require_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _require_positive_integer(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")


def _require_positive_number(value: float, field_name: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{field_name} must be a positive finite number")
    return float(value)


def _require_ratio(value: float, field_name: str) -> None:
    ratio = _require_positive_number(value, field_name)
    if ratio >= 1:
        raise ValueError(f"{field_name} must be less than 1")


def _bounded_retry_delay(config: DeliveryConfig, ordinal: int) -> float:
    exponent = min(
        max(0, ordinal - 1),
        config.local_pressure_exponent_limit,
    )
    if exponent == 0 or config.retry_multiplier == 1:
        return config.retry_initial_seconds
    log_multiplier = log(config.retry_multiplier)
    remaining_log_growth = (
        log(config.retry_max_seconds) - log(config.retry_initial_seconds)
    )
    if exponent >= remaining_log_growth / log_multiplier:
        return config.retry_max_seconds
    delay = exp(
        log(config.retry_initial_seconds) + float(exponent) * log_multiplier
    )
    return min(delay, config.retry_max_seconds)


__all__ = [
    "DEFAULT_DELIVERY_ITEM_LIMIT",
    "DEFAULT_DELIVERY_MAX_ATTEMPTS",
    "DEFAULT_DELIVERY_RECOVERY_BATCH_SIZE",
    "DEFAULT_DELIVERY_SOFT_LIMIT_RATIO",
    "DEFAULT_DELIVERY_STORAGE_BYTES",
    "DEFAULT_DELIVERY_WORK_BATCH_SIZE",
    "DeliveryConfig",
    "DeliverySemantics",
    "TopicDeliveryPolicy",
]
