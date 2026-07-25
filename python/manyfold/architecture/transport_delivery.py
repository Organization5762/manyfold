"""Bounded durable application delivery over :mod:`manyfold` TCP transport."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from pathlib import Path
from queue import Empty, Full, Queue
from threading import Condition, Event, Lock, Thread
from time import monotonic, time
from typing import final
from uuid import uuid4

from ._transport_delivery_journal import (
    _DeliveryJournal,
    _InboxDisposition,
    _InboxRecord,
    _JournalConflict,
    _JournalError,
    _JournalFull,
    _JournalStats,
    _OutboxRecord,
)
from ._transport_delivery_protocol import (
    _DELIVERY_HEADER_SIZE,
    _MAX_MESSAGE_ID_BYTES,
    _MAX_TEXT_BYTES,
    DELIVERY_CHANNEL,
    DELIVERY_PROTOCOL_VERSION,
    DeliveryError,
    DeliveryProtocolError,
    _decode_delivery_frame,
    _DeliveryFrame,
    _DeliveryOperation,
    _encode_delivery_frame,
)
from .transport import (
    FrameKind,
    LinkState,
    TcpTransport,
    TransportClosed,
    TransportMessage,
    TransportQueueFull,
)

DEFAULT_DELIVERY_ITEM_LIMIT = 1024
DEFAULT_DELIVERY_STORAGE_BYTES = 64 * 1024 * 1024

_WORK_BATCH_LIMIT = 32
_WORK_POLL_SECONDS = 0.05
_CLOSED_SENTINEL = object()


class DeliveryClosed(DeliveryError):
    """Raised when an operation targets a closed delivery layer."""


class DeliveryStorageFull(DeliveryError):
    """Raised when a configured journal item or byte bound is full."""


class DeliveryConflict(DeliveryError):
    """Raised when one stable message ID names different content."""


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
        if self.max_storage_bytes < 64 * 1024:
            raise ValueError("max_storage_bytes must be at least 65536")
        if self.retry_multiplier < 1:
            raise ValueError("retry_multiplier must be at least 1")
        if self.retry_max_seconds < self.retry_initial_seconds:
            raise ValueError(
                "retry_max_seconds must be at least retry_initial_seconds"
            )


@dataclass(frozen=True, slots=True)
class ReceivedDelivery:
    """One durable application message awaiting explicit ACK or NACK."""

    message_id: str
    message: TransportMessage
    delivery_attempt: int


@dataclass(frozen=True, slots=True)
class DeliveryHealth:
    """Immutable delivery, retry, retention, and journal health snapshot."""

    generation: int
    closed: bool
    outbox_items: int
    pending_inbox_items: int
    acked_inbox_items: int
    logical_storage_bytes: int
    queued_deliveries: int
    inflight_deliveries: int
    accepted: int
    frames_sent: int
    retries: int
    delivered: int
    acknowledgements: int
    negative_acknowledgements: int
    duplicates_suppressed: int
    expired_outbox: int
    expired_inbox: int
    last_error: str | None


@final
class DurableDelivery:
    """SQLite-backed at-least-once delivery over one owned receive stream."""

    def __init__(
        self,
        transport: TcpTransport,
        config: DeliveryConfig,
        *,
        owns_transport: bool = False,
    ) -> None:
        if not isinstance(transport, TcpTransport):
            raise ValueError("transport must be a TcpTransport")
        if not isinstance(config, DeliveryConfig):
            raise ValueError("config must be a DeliveryConfig")
        if not isinstance(owns_transport, bool):
            raise ValueError("owns_transport must be a boolean")
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
        try:
            self._journal = _DeliveryJournal(
                config.journal_path,
                max_outbox_items=config.max_outbox_items,
                max_inbox_items=config.max_inbox_items,
                max_storage_bytes=config.max_storage_bytes,
            )
        except _JournalFull as error:
            raise DeliveryStorageFull(str(error)) from error
        except _JournalError as error:
            raise DeliveryError(f"could not open delivery journal: {error}") from error
        self._received: Queue[ReceivedDelivery | object] = Queue(
            maxsize=config.receive_queue_limit
        )
        self._queued_ids: set[str] = set()
        self._inflight_ids: set[str] = set()
        self._condition = Condition(Lock())
        self._wake_sender = Event()
        self._stop = Event()
        self._closed = False
        self._generation = 0
        self._accepted = 0
        self._frames_sent = 0
        self._retries = 0
        self._delivered = 0
        self._acknowledgements = 0
        self._negative_acknowledgements = 0
        self._duplicates_suppressed = 0
        self._expired_outbox = 0
        self._expired_inbox = 0
        self._last_error: str | None = None
        self._last_stats = self._journal.stats()
        self._fill_receive_queue()
        self._receiver = Thread(
            target=self._run_receiver,
            name=f"manyfold-delivery-{transport.identity.node_id}-receiver",
            daemon=True,
        )
        self._sender = Thread(
            target=self._run_sender,
            name=f"manyfold-delivery-{transport.identity.node_id}-sender",
            daemon=True,
        )
        self._receiver.start()
        self._sender.start()

    def __enter__(self) -> "DurableDelivery":
        return self

    def __exit__(self, *error: object) -> None:
        self.close()

    def send(
        self,
        message: TransportMessage,
        *,
        message_id: str | None = None,
        ttl_seconds: float | None = None,
    ) -> str:
        """Durably retain one message before scheduling network delivery."""
        self._require_open()
        if not isinstance(message, TransportMessage):
            raise TypeError("message must be a TransportMessage")
        if message.sequence != 0:
            raise ValueError("outbound durable message sequence must be zero")
        if len(message.payload) > self.config.max_message_bytes:
            raise ValueError(
                "payload exceeds configured max_message_bytes "
                f"({len(message.payload)} > {self.config.max_message_bytes})"
            )
        resolved_message_id = (
            uuid4().hex
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
        resolved_ttl = (
            self.config.message_ttl_seconds
            if ttl_seconds is None
            else _require_positive_number(ttl_seconds, "ttl_seconds")
        )
        now = time()
        record = _OutboxRecord(
            message_id=resolved_message_id,
            frame_kind=int(message.kind),
            channel=message.channel,
            correlation_id=message.correlation_id,
            payload=message.payload,
            attempts=0,
        )
        try:
            inserted = self._journal.insert_outbox(
                record,
                created_at=now,
                expires_at=now + resolved_ttl,
            )
        except _JournalFull as error:
            raise DeliveryStorageFull(str(error)) from error
        except _JournalConflict as error:
            raise DeliveryConflict(str(error)) from error
        except _JournalError as error:
            raise DeliveryError(f"could not retain outbound message: {error}") from error
        if inserted:
            self._change(accepted=1)
        self._wake_sender.set()
        return resolved_message_id

    def receive(self, *, timeout: float | None = None) -> ReceivedDelivery:
        """Return one durable message for application processing."""
        _require_optional_timeout(timeout)
        deadline = None if timeout is None else monotonic() + timeout
        while True:
            remaining = (
                None if deadline is None else max(deadline - monotonic(), 0.0)
            )
            try:
                item = self._received.get(
                    timeout=_WORK_POLL_SECONDS if remaining is None else remaining
                )
            except Empty as error:
                if self._stop.is_set():
                    raise DeliveryClosed("durable delivery is closed") from error
                if deadline is None:
                    continue
                raise TimeoutError(
                    "no durable delivery arrived before timeout"
                ) from error
            if item is _CLOSED_SENTINEL:
                self._received.task_done()
                raise DeliveryClosed("durable delivery is closed")
            if not isinstance(item, ReceivedDelivery):
                self._received.task_done()
                raise DeliveryProtocolError(
                    "receive queue contained an invalid delivery"
                )
            try:
                with self._condition:
                    is_pending = self._journal.is_pending_inbox(
                        item.message_id,
                        time(),
                    )
                    self._queued_ids.discard(item.message_id)
                    if is_pending:
                        self._inflight_ids.add(item.message_id)
                    self._generation += 1
                    self._condition.notify_all()
            except _JournalError as error:
                self._received.task_done()
                raise DeliveryError(
                    f"could not receive message_id {item.message_id!r}: {error}"
                ) from error
            self._received.task_done()
            if not is_pending:
                continue
            self._fill_receive_queue()
            return item

    def ack(self, message_id: str) -> None:
        """Persist successful application processing and schedule an ACK."""
        message_id = _require_text(message_id, "message_id")
        self._require_inflight(message_id)
        now = time()
        try:
            was_marked = self._journal.mark_inbox_acked(
                message_id,
                next_ack_at=now,
            )
        except _JournalError as error:
            raise DeliveryError(
                f"could not ACK message_id {message_id!r}: {error}"
            ) from error
        if not was_marked:
            raise DeliveryConflict(
                f"inflight message_id {message_id!r} is not pending in the inbox"
            )
        with self._condition:
            self._inflight_ids.remove(message_id)
        self._change(acknowledgements=1)
        self._wake_sender.set()
        self._fill_receive_queue()

    def nack(self, message_id: str, *, reason: str = "application_rejected") -> None:
        """Release a pending inbox record and request sender retry."""
        message_id = _require_text(message_id, "message_id")
        reason = _require_text(reason, "reason")
        reason_bytes = reason.encode("utf-8")
        if len(reason_bytes) > _MAX_TEXT_BYTES:
            raise ValueError("encoded NACK reason is too long")
        self._require_inflight(message_id)
        try:
            was_deleted = self._journal.delete_pending_inbox(message_id)
        except _JournalError as error:
            raise DeliveryError(
                f"could not NACK message_id {message_id!r}: {error}"
            ) from error
        if not was_deleted:
            raise DeliveryConflict(
                f"inflight message_id {message_id!r} is not pending in the inbox"
            )
        with self._condition:
            self._inflight_ids.remove(message_id)
        self._send_control(_DeliveryOperation.NACK, message_id, reason_bytes)
        self._change(negative_acknowledgements=1)
        self._fill_receive_queue()

    def flush(self, *, timeout: float | None = None) -> bool:
        """Wait until every durable outbox record is acknowledged or expired."""
        _require_optional_timeout(timeout)
        deadline = None if timeout is None else monotonic() + timeout
        while self._journal_stats().outbox_items:
            with self._condition:
                if self._closed:
                    return False
                remaining = (
                    None if deadline is None else max(deadline - monotonic(), 0.0)
                )
                if remaining == 0:
                    return False
                self._condition.wait(
                    timeout=(
                        _WORK_POLL_SECONDS
                        if remaining is None
                        else min(_WORK_POLL_SECONDS, remaining)
                    )
                )
        return True

    def health(self) -> DeliveryHealth:
        """Return the latest immutable delivery and journal health."""
        stats = self._journal_stats()
        with self._condition:
            return DeliveryHealth(
                generation=self._generation,
                closed=self._closed,
                outbox_items=stats.outbox_items,
                pending_inbox_items=stats.pending_inbox_items,
                acked_inbox_items=stats.acked_inbox_items,
                logical_storage_bytes=stats.logical_bytes,
                queued_deliveries=len(self._queued_ids),
                inflight_deliveries=len(self._inflight_ids),
                accepted=self._accepted,
                frames_sent=self._frames_sent,
                retries=self._retries,
                delivered=self._delivered,
                acknowledgements=self._acknowledgements,
                negative_acknowledgements=self._negative_acknowledgements,
                duplicates_suppressed=self._duplicates_suppressed,
                expired_outbox=self._expired_outbox,
                expired_inbox=self._expired_inbox,
                last_error=self._last_error,
            )

    def wait_for_health_change(
        self,
        after_generation: int,
        *,
        timeout: float | None = None,
    ) -> DeliveryHealth:
        """Wait for delivery health to advance beyond one generation."""
        if (
            isinstance(after_generation, bool)
            or not isinstance(after_generation, int)
            or after_generation < 0
        ):
            raise ValueError("after_generation must be a non-negative integer")
        _require_optional_timeout(timeout)
        with self._condition:
            changed = self._condition.wait_for(
                lambda: self._generation > after_generation,
                timeout=timeout,
            )
        if not changed:
            raise TimeoutError("delivery health did not change before timeout")
        return self.health()

    def close(self, *, graceful_timeout: float = 0.0) -> None:
        """Stop workers, release queued payloads, and close the SQLite journal."""
        _require_nonnegative_number(graceful_timeout, "graceful_timeout")
        if graceful_timeout:
            self.flush(timeout=graceful_timeout)
        with self._condition:
            if self._closed:
                return
            self._closed = True
            self._generation += 1
            self._condition.notify_all()
        self._stop.set()
        self._wake_sender.set()
        self._receiver.join()
        self._sender.join()
        self._drain_receive_queue()
        self._last_stats = self._journal_stats()
        self._journal.close()
        if self._owns_transport:
            self.transport.close()

    def _run_receiver(self) -> None:
        while not self._stop.is_set():
            try:
                message = self.transport.receive(timeout=0.1)
            except TimeoutError:
                continue
            except TransportClosed as error:
                if not self._stop.is_set():
                    self._change(error=f"{type(error).__name__}: {error}")
                return
            try:
                frame = _decode_delivery_frame(
                    message,
                    max_message_bytes=self.config.max_message_bytes,
                )
                self._handle_frame(frame)
            except Exception as error:
                # Keep the owned receive worker observable until explicit disposal.
                self._change(error=f"{type(error).__name__}: {error}")

    def _run_sender(self) -> None:
        next_compaction = monotonic()
        while not self._stop.is_set():
            try:
                now_monotonic = monotonic()
                if now_monotonic >= next_compaction:
                    self._compact()
                    next_compaction = now_monotonic + 0.5
                if self.transport.health().state is LinkState.CONNECTED:
                    self._send_due_outbox()
                    self._send_due_acks()
            except Exception as error:
                # Journal or socket failures remain visible and retryable.
                self._change(error=f"{type(error).__name__}: {error}")
            self._wake_sender.wait(_WORK_POLL_SECONDS)
            self._wake_sender.clear()

    def _send_due_outbox(self) -> None:
        now = time()
        for record in self._journal.due_outbox(now, limit=_WORK_BATCH_LIMIT):
            frame = _encode_delivery_frame(
                _DeliveryOperation.DATA,
                record.message_id,
                frame_kind=record.frame_kind,
                channel=record.channel,
                correlation_id=record.correlation_id,
                payload=record.payload,
                delivery_attempt=record.attempts + 1,
            )
            try:
                self.transport.send(
                    TransportMessage(FrameKind.PUBSUB, DELIVERY_CHANNEL, frame),
                    timeout=0.01,
                )
            except (TransportClosed, TransportQueueFull) as error:
                self._journal.mark_outbox_attempt(
                    record.message_id,
                    next_attempt_at=now + self.config.retry_initial_seconds,
                    error=f"{type(error).__name__}: {error}",
                    increment_attempts=False,
                )
                self._change(error=f"{type(error).__name__}: {error}")
                continue
            attempt = record.attempts + 1
            self._journal.mark_outbox_attempt(
                record.message_id,
                next_attempt_at=now + self._retry_delay(attempt),
                error=None,
                increment_attempts=True,
            )
            self._change(frames_sent=1, retries=1 if record.attempts else 0)

    def _send_due_acks(self) -> None:
        now = time()
        for record in self._journal.due_acks(now, limit=_WORK_BATCH_LIMIT):
            if not self._send_control(_DeliveryOperation.ACK, record.message_id):
                continue
            attempt = record.ack_attempts + 1
            self._journal.mark_ack_attempt(
                record.message_id,
                next_ack_at=now + self._retry_delay(attempt),
            )

    def _handle_frame(self, frame: "_DeliveryFrame") -> None:
        if frame.operation is _DeliveryOperation.DATA:
            self._handle_data(frame)
        elif frame.operation is _DeliveryOperation.ACK:
            deleted = self._journal.delete_outbox(frame.message_id)
            self._send_control(_DeliveryOperation.CONFIRM, frame.message_id)
            if deleted:
                self._change()
        elif frame.operation is _DeliveryOperation.NACK:
            reason = frame.payload.decode("utf-8", errors="replace")
            self._journal.mark_outbox_attempt(
                frame.message_id,
                next_attempt_at=time(),
                error=f"peer NACK: {reason}",
                increment_attempts=False,
            )
            self._wake_sender.set()
            self._change(error=f"peer NACK for {frame.message_id!r}: {reason}")
        elif frame.operation is _DeliveryOperation.CONFIRM:
            self._journal.confirm_ack(frame.message_id)
            self._change()

    def _handle_data(self, frame: "_DeliveryFrame") -> None:
        record = _InboxRecord(
            message_id=frame.message_id,
            frame_kind=frame.frame_kind,
            channel=frame.channel,
            correlation_id=frame.correlation_id,
            payload=frame.payload,
            delivery_attempt=frame.delivery_attempt,
        )
        now = time()
        try:
            disposition = self._journal.record_inbox(
                record,
                created_at=now,
                expires_at=now + self.config.dedupe_retention_seconds,
            )
        except _JournalFull as error:
            self._send_control(
                _DeliveryOperation.NACK,
                frame.message_id,
                str(error).encode("utf-8"),
            )
            self._change(error=str(error))
            return
        except _JournalConflict as error:
            self._send_control(
                _DeliveryOperation.NACK,
                frame.message_id,
                str(error).encode("utf-8"),
            )
            raise DeliveryConflict(str(error)) from error
        if disposition is _InboxDisposition.ACKED_DUPLICATE:
            self._journal.schedule_ack_now(frame.message_id, now)
            self._wake_sender.set()
            self._change(duplicates_suppressed=1)
            return
        if disposition is _InboxDisposition.PENDING_DUPLICATE:
            self._change(duplicates_suppressed=1)
        else:
            self._change(delivered=1)
        self._fill_receive_queue()

    def _send_control(
        self,
        operation: "_DeliveryOperation",
        message_id: str,
        payload: bytes = b"",
    ) -> bool:
        frame = _encode_delivery_frame(
            operation,
            message_id,
            payload=payload,
        )
        try:
            self.transport.send(
                TransportMessage(FrameKind.PUBSUB, DELIVERY_CHANNEL, frame),
                timeout=0.01,
            )
        except (TransportClosed, TransportQueueFull) as error:
            self._change(error=f"{type(error).__name__}: {error}")
            return False
        self._change(frames_sent=1)
        return True

    def _fill_receive_queue(self) -> None:
        available = self.config.receive_queue_limit - self._received.qsize()
        if available <= 0:
            return
        try:
            records = self._journal.pending_inbox(
                time(),
                limit=self.config.max_inbox_items,
            )
        except _JournalError:
            return
        for record in records:
            with self._condition:
                if (
                    record.message_id in self._queued_ids
                    or record.message_id in self._inflight_ids
                ):
                    continue
                delivery = ReceivedDelivery(
                    message_id=record.message_id,
                    message=TransportMessage(
                        FrameKind(record.frame_kind),
                        record.channel,
                        record.payload,
                        correlation_id=record.correlation_id,
                    ),
                    delivery_attempt=record.delivery_attempt,
                )
                try:
                    self._received.put_nowait(delivery)
                except Full:
                    return
                self._queued_ids.add(record.message_id)
                self._generation += 1
                self._condition.notify_all()
                available -= 1
                if available == 0:
                    return

    def _compact(self) -> None:
        try:
            result = self._journal.compact(time())
        except _JournalError as error:
            self._change(error=f"{type(error).__name__}: {error}")
            return
        expired_outbox = len(result.expired_outbox_ids)
        expired_inbox = len(result.expired_inbox_ids)
        if expired_inbox:
            self._purge_expired_inbox(result.expired_inbox_ids)
        if expired_outbox or expired_inbox:
            self._change(
                expired_outbox=expired_outbox,
                expired_inbox=expired_inbox,
                error=(
                    f"expired {expired_outbox} outbox and "
                    f"{expired_inbox} inbox records"
                ),
            )

    def _purge_expired_inbox(self, message_ids: tuple[str, ...]) -> None:
        expired = frozenset(message_ids)
        retained: deque[ReceivedDelivery | object] = deque(
            maxlen=self.config.receive_queue_limit
        )
        with self._condition:
            while True:
                try:
                    item = self._received.get_nowait()
                except Empty:
                    break
                self._received.task_done()
                if (
                    isinstance(item, ReceivedDelivery)
                    and item.message_id in expired
                ):
                    continue
                retained.append(item)
            for item in retained:
                self._received.put_nowait(item)
            self._queued_ids.difference_update(expired)
            self._inflight_ids.difference_update(expired)
            self._generation += 1
            self._condition.notify_all()

    def _retry_delay(self, attempt: int) -> float:
        try:
            delay = self.config.retry_initial_seconds * (
                self.config.retry_multiplier ** max(attempt - 1, 0)
            )
        except OverflowError:
            return self.config.retry_max_seconds
        return min(delay, self.config.retry_max_seconds)

    def _require_inflight(self, message_id: str) -> None:
        self._require_open()
        with self._condition:
            if message_id not in self._inflight_ids:
                raise DeliveryConflict(
                    f"message_id {message_id!r} is not awaiting application result"
                )

    def _require_open(self) -> None:
        with self._condition:
            if self._closed:
                raise DeliveryClosed("durable delivery is closed")

    def _journal_stats(self) -> _JournalStats:
        try:
            stats = self._journal.stats()
        except _JournalError:
            return self._last_stats
        self._last_stats = stats
        return stats

    def _change(
        self,
        *,
        accepted: int = 0,
        frames_sent: int = 0,
        retries: int = 0,
        delivered: int = 0,
        acknowledgements: int = 0,
        negative_acknowledgements: int = 0,
        duplicates_suppressed: int = 0,
        expired_outbox: int = 0,
        expired_inbox: int = 0,
        error: str | None = None,
    ) -> None:
        with self._condition:
            self._accepted += accepted
            self._frames_sent += frames_sent
            self._retries += retries
            self._delivered += delivered
            self._acknowledgements += acknowledgements
            self._negative_acknowledgements += negative_acknowledgements
            self._duplicates_suppressed += duplicates_suppressed
            self._expired_outbox += expired_outbox
            self._expired_inbox += expired_inbox
            if error is not None:
                self._last_error = error
            self._generation += 1
            self._condition.notify_all()

    def _drain_receive_queue(self) -> None:
        while True:
            try:
                self._received.get_nowait()
            except Empty:
                break
            self._received.task_done()
        with self._condition:
            self._queued_ids.clear()
            self._inflight_ids.clear()
        try:
            self._received.put_nowait(_CLOSED_SENTINEL)
        except Full:
            pass


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


def _require_nonnegative_number(value: float, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative number")


def _require_optional_timeout(value: float | None) -> None:
    if value is not None:
        _require_nonnegative_number(value, "timeout")


__all__ = [
    "DEFAULT_DELIVERY_ITEM_LIMIT",
    "DEFAULT_DELIVERY_STORAGE_BYTES",
    "DELIVERY_CHANNEL",
    "DELIVERY_PROTOCOL_VERSION",
    "DeliveryClosed",
    "DeliveryConfig",
    "DeliveryConflict",
    "DeliveryError",
    "DeliveryHealth",
    "DeliveryProtocolError",
    "DeliveryStorageFull",
    "DurableDelivery",
    "ReceivedDelivery",
]
