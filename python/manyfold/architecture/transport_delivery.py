"""Bounded durable application delivery over one owned transport stream."""

from __future__ import annotations

import sqlite3
import warnings
from collections.abc import Iterator
from contextlib import contextmanager
from math import isfinite
from threading import Lock
from time import monotonic, time
from typing import final

from ._transport_delivery_events import (
    DeliveryCapacity,
    DeliveryCapacityDimension,
    DeliveryClosed,
    DeliveryCloseFailed,
    DeliveryConflict,
    DeliveryError,
    DeliveryEvent,
    DeliveryEventKind,
    DeliveryHealth,
    DeliveryObserver,
    DeliveryOutcome,
    DeliveryOutcomeKind,
    DeliveryProtocolError,
    DeliveryReceiveValidator,
    DeliveryStorageFull,
    DeliveryStore,
    DeliveryTopicHealth,
    ReceivedDelivery,
)
from ._transport_delivery_journal import _DeliveryJournal
from ._transport_delivery_journal_errors import (
    _JournalConflict,
    _JournalError,
    _JournalFull,
)
from ._transport_delivery_lifecycle import (
    _emit_committed_compaction,
    _emit_committed_watermark,
)
from ._transport_delivery_policy import (
    DEFAULT_DELIVERY_ITEM_LIMIT,
    DEFAULT_DELIVERY_MAX_ATTEMPTS,
    DEFAULT_DELIVERY_RECOVERY_BATCH_SIZE,
    DEFAULT_DELIVERY_SOFT_LIMIT_RATIO,
    DEFAULT_DELIVERY_STORAGE_BYTES,
    DEFAULT_DELIVERY_WORK_BATCH_SIZE,
    DeliveryConfig,
    DeliverySemantics,
    TopicDeliveryPolicy,
)
from ._transport_delivery_protocol import (
    _DELIVERY_HEADER_SIZE,
    _MAX_MESSAGE_ID_BYTES,
    DELIVERY_CHANNEL,
    DELIVERY_PROTOCOL_VERSION,
    _DeliveryOperation,
    _encode_delivery_frame,
)
from ._transport_delivery_receiver import _DeliveryReceiver
from ._transport_delivery_records import (
    _JournalStats,
    _OutboxDisposition,
    _OutboxRecord,
    _ReplayCursor,
)
from ._transport_delivery_recovery import _RecoveredStoreSide
from ._transport_delivery_runtime import _DeliveryRuntime
from ._transport_delivery_sender import _DeliverySender
from .transport import TcpTransport, TransportMessage


@final
class DurableDelivery:
    """SQLite-backed delivery with one receive loop and explicit ACK outcomes.

    Observer callbacks run synchronously in strict sequence order. They may
    inspect health from the callback; mutation, blocking operations, and close
    are rejected so one committed transition cannot be changed between its
    causal events. Callback failures are isolated into ``last_error``. No
    observer queue or thread is retained.
    """

    def __init__(
        self,
        transport: TcpTransport,
        config: DeliveryConfig,
        *,
        owns_transport: bool = False,
        observer: DeliveryObserver | None = None,
        receive_validator: DeliveryReceiveValidator | None = None,
    ) -> None:
        if not isinstance(transport, TcpTransport):
            raise ValueError("transport must be a TcpTransport")
        if not isinstance(config, DeliveryConfig):
            raise ValueError("config must be a DeliveryConfig")
        if not isinstance(owns_transport, bool):
            raise ValueError("owns_transport must be a boolean")
        if observer is not None and not callable(observer):
            raise ValueError("observer must be callable")
        if receive_validator is not None and not callable(receive_validator):
            raise ValueError("receive_validator must be callable")
        if config.max_message_bytes + _DELIVERY_HEADER_SIZE + 256 > (
            transport.config.max_payload_bytes
        ):
            raise ValueError(
                "max_message_bytes plus delivery framing exceeds transport "
                "max_payload_bytes"
            )
        self.transport = transport
        self.config = config
        self._owns_transport = owns_transport
        self._runtime = _DeliveryRuntime(observer)
        self._close_lock = Lock()
        self._journal_released = False
        self._last_stats = _JournalStats(0, 0, 0, 0, 0, 0, 0, 0)
        try:
            self._journal = _DeliveryJournal(config)
        except _JournalFull as error:
            raise DeliveryStorageFull(str(error)) from error
        except _JournalError as error:
            raise DeliveryError(
                f"could not open delivery journal: {error}"
            ) from error
        sender_started = False
        receiver_started = False
        try:
            self._recover()
            self._sender = _DeliverySender(
                transport,
                config,
                self._journal,
                self._runtime,
            )
            self._receiver = _DeliveryReceiver(
                transport,
                config,
                self._journal,
                self._runtime,
                self._sender,
                receive_validator,
            )
            self._runtime.set_unavailable_waker(
                self._receiver.wake_receivers
            )
            self._receiver.start()
            receiver_started = True
            self._sender.start()
            sender_started = True
        except BaseException as startup_error:
            self._runtime.start_close()
            receiver = getattr(self, "_receiver", None)
            if receiver is not None:
                receiver.wake_receivers()
            deadline = monotonic() + config.worker_join_timeout_seconds
            receiver_stopped = (
                True
                if not receiver_started
                else self._receiver.join(
                    max(0.0, deadline - monotonic())
                )
            )
            sender_stopped = (
                True
                if not sender_started
                else self._sender.join(
                    max(0.0, deadline - monotonic())
                )
            )
            if not receiver_stopped or not sender_stopped:
                raise DeliveryCloseFailed(
                    "delivery startup failed and a worker did not stop; "
                    "journal ownership remains live"
                ) from startup_error
            if receiver is not None:
                receiver.dispose()
            sender = getattr(self, "_sender", None)
            if sender is not None:
                sender.dispose()
            self._runtime.dispose_observer()
            self._journal.close()
            self._journal_released = True
            if isinstance(startup_error, _JournalFull):
                raise DeliveryStorageFull(str(startup_error)) from startup_error
            if isinstance(startup_error, _JournalError):
                raise DeliveryError(
                    f"could not recover delivery journal: {startup_error}"
                ) from startup_error
            raise

    def __enter__(self) -> DurableDelivery:
        return self

    def __exit__(self, *error: object) -> None:
        self.close()

    def send(
        self,
        message: TransportMessage,
        *,
        message_id: str | None = None,
        source: str | None = None,
        ttl_seconds: float | None = None,
    ) -> str:
        """Commit one explicitly configured durable message before sending."""
        self._runtime.require_callback_read_only()
        with _translate_journal_errors():
            with self._runtime.operation():
                return self._send_operation(
                    message,
                    message_id=message_id,
                    source=source,
                    ttl_seconds=ttl_seconds,
                )

    def receive(self, *, timeout: float | None = None) -> ReceivedDelivery:
        """Return one pending application delivery."""
        self._runtime.require_callback_read_only()
        _require_optional_timeout(timeout)
        with _translate_journal_errors():
            with self._runtime.operation():
                return self._receiver.receive(timeout=timeout)

    def ack(self, message_id: str) -> None:
        """Persist an ACK before scheduling its bounded wire response."""
        self._runtime.require_callback_read_only()
        with _translate_journal_errors():
            with self._runtime.operation():
                self._receiver.ack(_require_text(message_id, "message_id"))

    def nack(
        self,
        message_id: str,
        *,
        outcome: DeliveryOutcome | None = None,
        reason: str | None = None,
    ) -> None:
        """Persist or release one typed negative application outcome.

        ``reason=`` is a deprecated compatibility path and always means
        retryable. Use ``outcome=DeliveryOutcome.terminal(...)`` for a terminal
        rejection; the compatibility path cannot create an ambiguous terminal
        result.
        """
        self._runtime.require_callback_read_only()
        with _translate_journal_errors():
            with self._runtime.operation():
                self._nack_operation(
                    message_id,
                    outcome=outcome,
                    reason=reason,
                )

    def flush(self, *, timeout: float | None = None) -> bool:
        """Wait for all outbound durable rows to reach a final outcome."""
        self._runtime.require_callback_read_only()
        _require_optional_timeout(timeout)
        with _translate_journal_errors():
            with self._runtime.operation():
                return self._flush_operation(
                    timeout=timeout,
                    allow_closing=False,
                )

    def health(self) -> DeliveryHealth:
        """Return exact retained journal counts and bounded runtime counters."""
        with _translate_journal_errors():
            with self._runtime.transition():
                if (
                    not self._journal_released
                    and not self._runtime.stop.is_set()
                ):
                    self._last_stats = self._journal.stats()
                receiver = getattr(self, "_receiver", None)
                queued, inflight = (
                    (0, 0)
                    if self._journal_released or receiver is None
                    else receiver.counts()
                )
        return self._runtime.health(
            self._last_stats,
            queued_deliveries=queued,
            inflight_deliveries=inflight,
        )

    def topic_health(self, topic: str) -> DeliveryTopicHealth:
        """Return exact retained item and byte counts for one topic."""
        with _translate_journal_errors():
            with self._runtime.operation():
                normalized = _require_text(topic, "topic")
                stats = self._journal.topic_stats(normalized)
        return DeliveryTopicHealth(
            normalized,
            stats.outbox_items + stats.inbox_items,
            stats.outbox_items,
            stats.append_outbox_items,
            stats.latest_outbox_items,
            stats.inbox_items,
            stats.logical_bytes,
        )

    def wait_for_health_change(
        self,
        after_generation: int,
        *,
        timeout: float | None = None,
    ) -> DeliveryHealth:
        """Wait for a newer snapshot or raise if already closed at that state."""
        self._runtime.require_callback_read_only()
        if (
            isinstance(after_generation, bool)
            or not isinstance(after_generation, int)
            or after_generation < 0
        ):
            raise ValueError("after_generation must be a non-negative integer")
        _require_optional_timeout(timeout)
        self._runtime.wait_for_change(after_generation, timeout=timeout)
        return self.health()

    def close(self, *, graceful_timeout: float = 0.0) -> None:
        """Stop both workers before releasing the journal and owned transport."""
        self._runtime.require_callback_read_only()
        _require_nonnegative_number(graceful_timeout, "graceful_timeout")
        with _translate_journal_errors():
            self._close_operation(graceful_timeout)

    def _send_operation(
        self,
        message: TransportMessage,
        *,
        message_id: str | None,
        source: str | None,
        ttl_seconds: float | None,
    ) -> str:
        if not isinstance(message, TransportMessage):
            raise TypeError("message must be a TransportMessage")
        if message.sequence != 0:
            raise ValueError("outbound durable message sequence must be zero")
        if len(message.payload) > self.config.max_message_bytes:
            raise ValueError(
                "payload exceeds configured max_message_bytes "
                f"({len(message.payload)} > {self.config.max_message_bytes})"
            )
        policy = self.config.policy_for(message.channel)
        resolved_source = _resolve_source(policy, source)
        resolved_message_id = (
            self._journal.next_message_id()
            if message_id is None
            else _require_text(message_id, "message_id")
        )
        if len(resolved_message_id.encode("utf-8")) > _MAX_MESSAGE_ID_BYTES:
            raise ValueError("encoded message_id is too long")
        encoded = _encode_delivery_frame(
            _DeliveryOperation.DATA,
            resolved_message_id,
            frame_kind=int(message.kind),
            channel=message.channel,
            correlation_id=message.correlation_id,
            payload=message.payload,
            delivery_attempt=1,
        )
        if len(encoded) > self.transport.config.max_payload_bytes:
            raise ValueError(
                "encoded durable message exceeds transport max_payload_bytes "
                f"({len(encoded)} > {self.transport.config.max_payload_bytes})"
            )
        ttl = (
            policy.ttl_seconds
            if ttl_seconds is None
            else _require_positive_number(ttl_seconds, "ttl_seconds")
        )
        if ttl > policy.ttl_seconds:
            raise ValueError(
                "ttl_seconds cannot exceed the topic policy TTL "
                f"({ttl:g} > {policy.ttl_seconds:g})"
            )
        record = _OutboxRecord(
            resolved_message_id,
            message.channel,
            policy.semantics.value,
            resolved_source,
            int(message.kind),
            message.correlation_id,
            message.payload,
            0,
            policy.max_attempts,
        )
        with self._runtime.transition():
            now = time()
            try:
                inserted = self._journal.insert_outbox(
                    record,
                    created_at=now,
                    expires_at=now + ttl,
                    now=now,
                    policy=policy,
                )
            except _JournalFull as error:
                self._runtime.change(storage_rejections=1, error=str(error))
                raise DeliveryStorageFull(str(error)) from error
            except _JournalConflict as error:
                raise DeliveryConflict(str(error)) from error
            if inserted.crossing is not None:
                _emit_committed_watermark(
                    self._runtime,
                    topic=message.channel,
                    crossing=inserted.crossing,
                    store=DeliveryStore.OUTBOX,
                )
            if inserted.sweep is not None:
                self._receiver.observe_committed_compaction(
                    inserted.sweep,
                    capacity=inserted.capacity,
                    emit_empty=True,
                )
            if inserted.disposition is _OutboxDisposition.DEDUPLICATED:
                self._runtime.emit(
                    DeliveryEventKind.DEDUPLICATED,
                    resolved_message_id,
                    message.channel,
                    resolved_source,
                    store=DeliveryStore.OUTBOX,
                    correlation_id=message.correlation_id,
                )
                self._runtime.change(outbox_deduplicated=1)
            elif inserted.disposition is _OutboxDisposition.COALESCED:
                self._runtime.emit(
                    DeliveryEventKind.COALESCED,
                    resolved_message_id,
                    message.channel,
                    resolved_source,
                    store=DeliveryStore.OUTBOX,
                    correlation_id=message.correlation_id,
                    related_message_id=(
                        None
                        if inserted.replaced is None
                        else inserted.replaced.message_id
                    ),
                )
                self._runtime.change(accepted=1, coalesced=1)
            else:
                self._runtime.emit(
                    DeliveryEventKind.ENQUEUED,
                    resolved_message_id,
                    message.channel,
                    resolved_source,
                    store=DeliveryStore.OUTBOX,
                    correlation_id=message.correlation_id,
                )
                self._runtime.change(accepted=1)
        self._sender.wake()
        return resolved_message_id

    def _nack_operation(
        self,
        message_id: str,
        *,
        outcome: DeliveryOutcome | None,
        reason: str | None,
    ) -> None:
        if outcome is not None and reason is not None:
            raise ValueError("pass either outcome or deprecated reason, not both")
        if outcome is not None and not isinstance(outcome, DeliveryOutcome):
            raise TypeError("outcome must be a DeliveryOutcome")
        if reason is not None:
            warnings.warn(
                "nack(reason=...) is deprecated; pass a retryable "
                "DeliveryOutcome",
                DeprecationWarning,
                stacklevel=2,
            )
            outcome = DeliveryOutcome.retryable(reason)
        if outcome is None:
            outcome = DeliveryOutcome.retryable("application rejected")
        self._receiver.nack(
            _require_text(message_id, "message_id"),
            outcome,
        )

    def _flush_operation(
        self,
        *,
        timeout: float | None,
        allow_closing: bool,
    ) -> bool:
        deadline = None if timeout is None else monotonic() + timeout
        while True:
            if not allow_closing:
                self._require_open()
            snapshot = self.health()
            if snapshot.outbox_items == 0:
                remaining = (
                    None if deadline is None else max(0.0, deadline - monotonic())
                )
                return self.transport.flush(timeout=remaining)
            remaining = (
                None if deadline is None else max(0.0, deadline - monotonic())
            )
            if remaining == 0:
                return False
            try:
                self._runtime.wait_for_change(
                    snapshot.generation,
                    timeout=remaining,
                    allow_closing=allow_closing,
                )
            except TimeoutError:
                return False

    def _close_operation(self, graceful_timeout: float) -> None:
        with self._close_lock:
            if self._journal_released:
                return
            started_close = self._runtime.begin_close()
            self._receiver.wake_receivers()
            shutdown_deadline = (
                monotonic() + self.config.worker_join_timeout_seconds
            )
            operations_stopped = self._runtime.wait_for_operations(
                timeout=max(0.0, shutdown_deadline - monotonic())
            )
            graceful_error: Exception | None = None
            if started_close and graceful_timeout and operations_stopped:
                graceful_started_at = monotonic()
                try:
                    self._flush_operation(
                        timeout=graceful_timeout,
                        allow_closing=True,
                    )
                except Exception as error:
                    graceful_error = error
                finally:
                    # Graceful drain has its own caller-selected budget. Exclude
                    # it from the fixed operation/worker shutdown budget.
                    shutdown_deadline += monotonic() - graceful_started_at
            self._runtime.stop_workers()
            self._receiver.wake_receivers()
            receiver_stopped = self._receiver.join(
                max(0.0, shutdown_deadline - monotonic())
            )
            sender_stopped = self._sender.join(
                max(0.0, shutdown_deadline - monotonic())
            )
            if (
                not operations_stopped
                or not receiver_stopped
                or not sender_stopped
            ):
                raise DeliveryCloseFailed(
                    "delivery operations or workers did not stop within "
                    f"{self.config.worker_join_timeout_seconds:g}s; "
                    "journal ownership remains live"
                )
            cleanup_error: Exception | None = None
            with self._runtime.transition():
                if self._journal.has_open_connection():
                    try:
                        self._last_stats = self._journal.stats()
                    except Exception as error:
                        cleanup_error = error
                for cleanup in (
                    self._receiver.dispose,
                    self._sender.dispose,
                    self._runtime.dispose_observer,
                ):
                    try:
                        cleanup()
                    except Exception as error:
                        if cleanup_error is None:
                            cleanup_error = error
                try:
                    self._journal.close()
                except Exception as error:
                    if cleanup_error is None:
                        cleanup_error = error
                finally:
                    self._journal_released = self._journal.is_released()
            transport_error: Exception | None = None
            try:
                if self._owns_transport:
                    self.transport.close(graceful_timeout=0.0)
            except Exception as error:
                transport_error = error
            finally:
                if self._journal_released:
                    self._runtime.finish_close()
            if cleanup_error is not None:
                raise cleanup_error
            if graceful_error is not None:
                raise DeliveryError(
                    f"graceful delivery flush failed: {graceful_error}"
                ) from graceful_error
            if transport_error is not None:
                raise DeliveryError(
                    f"owned transport close failed: {transport_error}"
                ) from transport_error

    def _recover(self) -> None:
        policies = {
            policy.topic: policy for policy in self.config.topic_policies
        }
        self._journal.validate_recovery(
            policies,
            max_transport_payload_bytes=self.transport.config.max_payload_bytes,
            recovery_now=time(),
            enforce_bounds=False,
        )
        max_batches = (
            (
                self.config.max_outbox_items
                + self.config.max_inbox_items
                + self.config.recovery_batch_size
                - 1
            )
            // self.config.recovery_batch_size
        ) + 1
        for _ in range(max_batches):
            compacted = self._journal.compact(
                time(),
                limit=self.config.recovery_batch_size,
            )
            _emit_committed_compaction(self._runtime, compacted)
            if compacted.affected_items == 0:
                break
        else:
            raise DeliveryStorageFull(
                "startup expiry work exceeds configured retained item bounds"
            )
        recovered_watermarks = self._journal.validate_recovery(
            policies,
            max_transport_payload_bytes=self.transport.config.max_payload_bytes,
            recovery_now=time(),
        )
        # Recovery observers may inspect health before worker ownership exists.
        # Journal counts are exact; queue and inflight counts are explicitly zero.
        self._last_stats = self._journal.stats()
        for recovered in recovered_watermarks:
            self._runtime.emit(
                DeliveryEventKind.WATERMARK_RECOVERED,
                None,
                recovered.topic,
                None,
                store=(
                    None
                    if recovered.side is None
                    else (
                        DeliveryStore.OUTBOX
                        if recovered.side is _RecoveredStoreSide.OUTBOX
                        else DeliveryStore.INBOX
                    )
                ),
                capacity_dimension=recovered.dimension,
                capacity=recovered.capacity,
            )
            self._runtime.change(recovered_watermarks=1)
        cursor: _ReplayCursor | None = None
        while True:
            batch = self._journal.outbox_replay_batch(
                cursor,
                limit=self.config.recovery_batch_size,
            )
            if not batch:
                break
            for record in batch:
                self._runtime.emit(
                    DeliveryEventKind.REPLAYED,
                    record.message_id,
                    record.channel,
                    record.source_key,
                    store=DeliveryStore.OUTBOX,
                    correlation_id=record.correlation_id,
                    attempt=record.attempts,
                )
                cursor = record.cursor
            self._runtime.change(
                recovered_outbox=len(batch),
            )
        self._last_stats = self._journal.stats()

    def _require_open(self) -> None:
        if self._runtime.is_closed() or self._journal_released:
            raise DeliveryClosed("durable delivery is closed")


def _resolve_source(
    policy: TopicDeliveryPolicy,
    source: str | None,
) -> str | None:
    if policy.semantics is DeliverySemantics.LATEST:
        if source is None:
            raise ValueError(
                f"latest durable topic {policy.topic!r} requires source"
            )
        return _require_text(source, "source")
    if source is not None:
        raise ValueError(
            f"append durable topic {policy.topic!r} cannot carry source"
        )
    return None


def _require_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _require_positive_number(value: float, field_name: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{field_name} must be positive and finite")
    return float(value)


def _require_nonnegative_number(value: float, field_name: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not isfinite(value)
        or value < 0
    ):
        raise ValueError(f"{field_name} must be non-negative and finite")
    return float(value)


def _require_optional_timeout(value: float | None) -> None:
    if value is not None:
        _require_nonnegative_number(value, "timeout")


@contextmanager
def _translate_journal_errors() -> Iterator[None]:
    try:
        yield
    except _JournalFull as error:
        raise DeliveryStorageFull(str(error)) from error
    except _JournalConflict as error:
        raise DeliveryConflict(str(error)) from error
    except _JournalError as error:
        raise DeliveryError(str(error)) from error
    except sqlite3.DatabaseError as error:
        raise DeliveryError(f"delivery journal operation failed: {error}") from error


__all__ = [
    "DEFAULT_DELIVERY_ITEM_LIMIT",
    "DEFAULT_DELIVERY_MAX_ATTEMPTS",
    "DEFAULT_DELIVERY_RECOVERY_BATCH_SIZE",
    "DEFAULT_DELIVERY_SOFT_LIMIT_RATIO",
    "DEFAULT_DELIVERY_STORAGE_BYTES",
    "DEFAULT_DELIVERY_WORK_BATCH_SIZE",
    "DELIVERY_CHANNEL",
    "DELIVERY_PROTOCOL_VERSION",
    "DeliveryCapacity",
    "DeliveryCapacityDimension",
    "DeliveryCloseFailed",
    "DeliveryClosed",
    "DeliveryConfig",
    "DeliveryConflict",
    "DeliveryError",
    "DeliveryEvent",
    "DeliveryEventKind",
    "DeliveryHealth",
    "DeliveryObserver",
    "DeliveryOutcome",
    "DeliveryOutcomeKind",
    "DeliveryProtocolError",
    "DeliveryReceiveValidator",
    "DeliverySemantics",
    "DeliveryStorageFull",
    "DeliveryStore",
    "DeliveryTopicHealth",
    "DurableDelivery",
    "ReceivedDelivery",
    "TopicDeliveryPolicy",
]
