"""Explicit journaled and live-latest policies for :class:`TransportMesh`."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import final

from .transport_delivery import TopicDeliveryPolicy

DEFAULT_DURABLE_PEER_ITEMS = 1024
DEFAULT_DURABLE_PEER_BYTES = 64 * 1024 * 1024


@final
class TopicDeliveryClass(str, Enum):
    """A binding's transport and retention contract."""

    DURABLE_APPEND = "durable_append"
    DURABLE_LATEST = "durable_latest"
    LIVE_LATEST = "live_latest"


@final
@dataclass(frozen=True, slots=True)
class MeshTopicPolicy:
    """One named topic's journaled or process-local latest delivery policy."""

    topic: str
    delivery_class: TopicDeliveryClass
    max_message_bytes: int
    max_sources: int
    key_field: str | None = None
    journal_policy: TopicDeliveryPolicy | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "topic", _text(self.topic, "topic"))
        if not isinstance(self.delivery_class, TopicDeliveryClass):
            raise ValueError("delivery_class must be a TopicDeliveryClass")
        _positive_integer(self.max_message_bytes, "max_message_bytes")
        _positive_integer(self.max_sources, "max_sources")
        if self.key_field is not None:
            object.__setattr__(
                self,
                "key_field",
                _text(self.key_field, "key_field"),
            )
        if self.retains_journal_rows:
            if not isinstance(self.journal_policy, TopicDeliveryPolicy):
                raise ValueError("durable mesh topics require journal_policy")
            if self.journal_policy.topic != self.topic:
                raise ValueError("journal_policy topic must match mesh topic")
        elif self.journal_policy is not None:
            raise ValueError("live latest topics cannot configure a journal policy")

    @classmethod
    def commands(
        cls,
        topic: str,
        *,
        max_items: int = 256,
        max_bytes: int = 1024 * 1024,
        max_message_bytes: int = 16 * 1024,
        ttl_seconds: float = 10.0,
        max_attempts: int = 64,
        soft_limit_ratio: float = 0.7,
    ) -> "MeshTopicPolicy":
        """Build bounded durable append with stable-key deduplication."""
        return cls(
            topic,
            TopicDeliveryClass.DURABLE_APPEND,
            max_message_bytes,
            max_items,
            journal_policy=TopicDeliveryPolicy.commands(
                topic,
                max_items=max_items,
                max_bytes=max_bytes,
                ttl_seconds=ttl_seconds,
                max_attempts=max_attempts,
                soft_limit_ratio=soft_limit_ratio,
            ),
        )

    @classmethod
    def latest(
        cls,
        topic: str,
        *,
        max_sources: int,
        max_bytes: int,
        max_message_bytes: int = 512 * 1024,
        ttl_seconds: float,
        max_attempts: int = 4,
        soft_limit_ratio: float = 0.5,
        key_field: str | None = None,
    ) -> "MeshTopicPolicy":
        """Build bounded durable latest-per-source delivery."""
        return cls(
            topic,
            TopicDeliveryClass.DURABLE_LATEST,
            max_message_bytes,
            max_sources,
            key_field,
            TopicDeliveryPolicy.latest(
                topic,
                max_sources=max_sources,
                max_bytes=max_bytes,
                ttl_seconds=ttl_seconds,
                max_attempts=max_attempts,
                soft_limit_ratio=soft_limit_ratio,
            ),
        )

    @classmethod
    def live_latest(
        cls,
        topic: str,
        *,
        max_sources: int = 1,
        max_message_bytes: int = 512 * 1024,
        key_field: str | None = None,
    ) -> "MeshTopicPolicy":
        """Build non-journaled one-slot-per-source reconnect resynchronization."""
        return cls(
            topic,
            TopicDeliveryClass.LIVE_LATEST,
            max_message_bytes,
            max_sources,
            key_field,
        )

    @property
    def retains_journal_rows(self) -> bool:
        """Return whether this policy may persist delivery rows."""
        return self.delivery_class is not TopicDeliveryClass.LIVE_LATEST


@final
@dataclass(frozen=True, slots=True)
class MeshDurabilityConfig:
    """Per-peer durable journal and retry bounds."""

    journal_directory: Path
    hard_peer_items: int = DEFAULT_DURABLE_PEER_ITEMS
    hard_peer_bytes: int = DEFAULT_DURABLE_PEER_BYTES
    dedupe_retention_seconds: float = 10.0
    retry_initial_seconds: float = 0.02
    retry_multiplier: float = 1.5
    retry_max_seconds: float = 0.25

    def __post_init__(self) -> None:
        if not isinstance(self.journal_directory, Path):
            raise ValueError("journal_directory must be a pathlib.Path")
        _positive_integer(self.hard_peer_items, "hard_peer_items")
        _positive_integer(self.hard_peer_bytes, "hard_peer_bytes")
        _positive_number(
            self.dedupe_retention_seconds,
            "dedupe_retention_seconds",
        )
        _positive_number(self.retry_initial_seconds, "retry_initial_seconds")
        _positive_number(self.retry_multiplier, "retry_multiplier")
        _positive_number(self.retry_max_seconds, "retry_max_seconds")
        if self.retry_multiplier < 1:
            raise ValueError("retry_multiplier must be at least 1")
        if self.retry_max_seconds < self.retry_initial_seconds:
            raise ValueError("retry_max_seconds must be at least retry_initial_seconds")


@final
@dataclass(frozen=True, slots=True)
class DurableTopicDiagnostics:
    """Per-topic durable transition counters across current peers."""

    topic: str
    delivery_class: TopicDeliveryClass
    retains_journal_rows: bool
    outbox_items: int
    coalesced: int
    expired: int
    retried: int
    acknowledged: int
    storage_rejections: int
    recovery_loaded_rows: int


def _text(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _positive_integer(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")


def _positive_number(value: float, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float) or value <= 0:
        raise ValueError(f"{name} must be a positive number")


__all__ = [
    "DEFAULT_DURABLE_PEER_BYTES",
    "DEFAULT_DURABLE_PEER_ITEMS",
    "DurableTopicDiagnostics",
    "MeshDurabilityConfig",
    "MeshTopicPolicy",
    "TopicDeliveryClass",
]
