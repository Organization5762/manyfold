"""Public configuration and binding handles for durable mesh topics."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from threading import RLock
from typing import Callable, final

from .pubsub import PubSub
from .transport_delivery import (
    DEFAULT_DELIVERY_ITEM_LIMIT,
    DEFAULT_DELIVERY_STORAGE_BYTES,
    DeliveryConfig,
)


class MeshTopicPolicy(str, Enum):
    """Application retention semantics for one named durable topic."""

    APPEND = "append"
    LATEST = "latest"


@final
@dataclass(frozen=True, slots=True)
class MeshDeliveryConfig:
    """Per-peer durable-delivery limits configured once for one mesh."""

    state_directory: Path | None = None
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
    graceful_shutdown_seconds: float = 0.0

    def __post_init__(self) -> None:
        if self.state_directory is not None and not isinstance(
            self.state_directory,
            Path,
        ):
            raise ValueError("state_directory must be a pathlib.Path or None")
        if (
            isinstance(self.graceful_shutdown_seconds, bool)
            or not isinstance(self.graceful_shutdown_seconds, int | float)
            or self.graceful_shutdown_seconds < 0
        ):
            raise ValueError(
                "graceful_shutdown_seconds must be a non-negative number"
            )
        # Reuse the delivery layer's authoritative validation.
        self._for_journal(
            Path("validation.sqlite3"),
            transport_max_payload_bytes=self.max_message_bytes + 1024,
        )

    @property
    def is_restart_durable(self) -> bool:
        """Return whether peer journals survive mesh object disposal."""
        return self.state_directory is not None

    def _for_journal(
        self,
        journal_path: Path,
        *,
        transport_max_payload_bytes: int,
    ) -> DeliveryConfig:
        effective_message_bytes = min(
            self.max_message_bytes,
            max(1, transport_max_payload_bytes - 512),
        )
        return DeliveryConfig(
            journal_path=journal_path,
            max_outbox_items=self.max_outbox_items,
            max_inbox_items=self.max_inbox_items,
            max_storage_bytes=self.max_storage_bytes,
            receive_queue_limit=self.receive_queue_limit,
            max_message_bytes=effective_message_bytes,
            message_ttl_seconds=self.message_ttl_seconds,
            dedupe_retention_seconds=self.dedupe_retention_seconds,
            retry_initial_seconds=self.retry_initial_seconds,
            retry_multiplier=self.retry_multiplier,
            retry_max_seconds=self.retry_max_seconds,
        )


@final
class MeshTopicBinding:
    """Mesh-owned disposable binding between one named ``PubSubTopic`` and peers."""

    def __init__(
        self,
        topic: PubSub,
        policy: MeshTopicPolicy,
        dispose_callback: Callable[[], bool],
    ) -> None:
        self.topic = topic
        self.policy = policy
        self._dispose_callback: Callable[[], bool] | None = dispose_callback
        self._lock = RLock()

    @property
    def is_disposed(self) -> bool:
        """Return whether the topic binding has been removed."""
        with self._lock:
            return self._dispose_callback is None

    def __enter__(self) -> "MeshTopicBinding":
        return self

    def __exit__(self, *error: object) -> None:
        self.dispose()

    def dispose(self) -> bool:
        """Stop local forwarding and withdraw the durable mesh subscription."""
        with self._lock:
            if self._dispose_callback is None:
                return False
            dispose_callback = self._dispose_callback
            self._dispose_callback = None
        return dispose_callback()


__all__ = [
    "MeshDeliveryConfig",
    "MeshTopicBinding",
    "MeshTopicPolicy",
]
