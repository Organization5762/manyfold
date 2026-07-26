"""Bounded durable topic policies for :class:`TransportMesh`."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import final

DEFAULT_DURABLE_PEER_ITEMS = 1024
DEFAULT_DURABLE_PEER_BYTES = 64 * 1024 * 1024


@final
class DurableTopicMode(str, Enum):
    """The two supported outage-retention semantics."""

    APPEND = "append"
    LATEST = "latest"


@final
@dataclass(frozen=True, slots=True)
class DurableTopicPolicy:
    """Per-topic retention, pressure, and expiry limits."""

    mode: DurableTopicMode
    ttl_seconds: float
    soft_pending_items: int
    hard_pending_items: int
    soft_pending_bytes: int
    hard_pending_bytes: int
    max_message_bytes: int
    key_field: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.mode, DurableTopicMode):
            raise ValueError("mode must be a DurableTopicMode")
        _positive_number(self.ttl_seconds, "ttl_seconds")
        _positive_integer(self.soft_pending_items, "soft_pending_items")
        _positive_integer(self.hard_pending_items, "hard_pending_items")
        _positive_integer(self.soft_pending_bytes, "soft_pending_bytes")
        _positive_integer(self.hard_pending_bytes, "hard_pending_bytes")
        _positive_integer(self.max_message_bytes, "max_message_bytes")
        if self.soft_pending_items > self.hard_pending_items:
            raise ValueError("soft_pending_items must not exceed hard_pending_items")
        if self.soft_pending_bytes > self.hard_pending_bytes:
            raise ValueError("soft_pending_bytes must not exceed hard_pending_bytes")
        if self.max_message_bytes > self.hard_pending_bytes:
            raise ValueError("max_message_bytes must not exceed hard_pending_bytes")
        if self.key_field is not None:
            object.__setattr__(
                self,
                "key_field",
                _text(self.key_field, "key_field"),
            )
        if self.mode is DurableTopicMode.APPEND and self.key_field is not None:
            raise ValueError("append topics do not accept key_field")

    @classmethod
    def append(
        cls,
        *,
        ttl_seconds: float = 10.0,
        soft_pending_items: int = 192,
        hard_pending_items: int = 256,
        soft_pending_bytes: int = 768 * 1024,
        hard_pending_bytes: int = 1024 * 1024,
        max_message_bytes: int = 16 * 1024,
    ) -> "DurableTopicPolicy":
        """Retain distinct commands in order until ACK, expiry, or a hard cap."""
        return cls(
            DurableTopicMode.APPEND,
            ttl_seconds,
            soft_pending_items,
            hard_pending_items,
            soft_pending_bytes,
            hard_pending_bytes,
            max_message_bytes,
        )

    @classmethod
    def latest(
        cls,
        *,
        ttl_seconds: float,
        max_keys: int = 1,
        soft_pending_bytes: int = 512 * 1024,
        hard_pending_bytes: int = 1024 * 1024,
        max_message_bytes: int = 512 * 1024,
        key_field: str | None = None,
    ) -> "DurableTopicPolicy":
        """Keep one pending value for the topic or for each bounded key."""
        _positive_integer(max_keys, "max_keys")
        return cls(
            DurableTopicMode.LATEST,
            ttl_seconds,
            max_keys,
            max_keys,
            soft_pending_bytes,
            hard_pending_bytes,
            max_message_bytes,
            key_field,
        )


@final
@dataclass(frozen=True, slots=True)
class MeshDurabilityConfig:
    """Per-peer journal, retry, dedupe, and compaction bounds."""

    journal_directory: Path
    soft_peer_items: int = 768
    hard_peer_items: int = DEFAULT_DURABLE_PEER_ITEMS
    soft_peer_bytes: int = 48 * 1024 * 1024
    hard_peer_bytes: int = DEFAULT_DURABLE_PEER_BYTES
    dedupe_retention_seconds: float = 10.0
    retry_initial_seconds: float = 0.02
    retry_multiplier: float = 1.5
    retry_max_seconds: float = 0.25
    compaction_interval_seconds: float = 0.1

    def __post_init__(self) -> None:
        if not isinstance(self.journal_directory, Path):
            raise ValueError("journal_directory must be a pathlib.Path")
        _positive_integer(self.soft_peer_items, "soft_peer_items")
        _positive_integer(self.hard_peer_items, "hard_peer_items")
        _positive_integer(self.soft_peer_bytes, "soft_peer_bytes")
        _positive_integer(self.hard_peer_bytes, "hard_peer_bytes")
        if self.soft_peer_items > self.hard_peer_items:
            raise ValueError("soft_peer_items must not exceed hard_peer_items")
        if self.soft_peer_bytes > self.hard_peer_bytes:
            raise ValueError("soft_peer_bytes must not exceed hard_peer_bytes")
        _positive_number(
            self.dedupe_retention_seconds,
            "dedupe_retention_seconds",
        )
        _positive_number(self.retry_initial_seconds, "retry_initial_seconds")
        _positive_number(self.retry_multiplier, "retry_multiplier")
        _positive_number(self.retry_max_seconds, "retry_max_seconds")
        _positive_number(
            self.compaction_interval_seconds,
            "compaction_interval_seconds",
        )
        if self.retry_multiplier < 1:
            raise ValueError("retry_multiplier must be at least 1")
        if self.retry_max_seconds < self.retry_initial_seconds:
            raise ValueError("retry_max_seconds must be at least retry_initial_seconds")


@final
@dataclass(frozen=True, slots=True)
class DurableTopicDiagnostics:
    """Bounded durable-topic counters and retained rows."""

    topic: str
    mode: DurableTopicMode
    outbox_items: int
    pending_inbox_items: int
    acked_inbox_items: int
    logical_bytes: int
    replaced: int
    expired: int
    retried: int
    acknowledged: int
    hard_cap_rejected: int
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
    "DurableTopicMode",
    "DurableTopicPolicy",
    "MeshDurabilityConfig",
]
