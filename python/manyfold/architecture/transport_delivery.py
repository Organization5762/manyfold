"""Bounded durable application delivery over :mod:`manyfold` TCP transport."""

from __future__ import annotations

from collections import deque
from queue import Empty, Full, Queue
from threading import Condition, Event, Lock, Thread
from time import monotonic, time
from typing import final

from ._transport_delivery_events import (
    DeliveryCapacity,
    DeliveryClosed,
    DeliveryConflict,
    DeliveryEvent,
    DeliveryEventKind,
    DeliveryHealth,
    DeliveryObserver,
    DeliveryReceiveValidator,
    DeliveryStorageFull,
    DeliveryTopicHealth,
    ReceivedDelivery,
)
from ._transport_delivery_journal import (
    _DeliveryJournal,
    _InboxDisposition,
    _InboxRecord,
    _JournalConflict,
    _JournalError,
    _JournalFull,
    _JournalStats,
    _OutboxDisposition,
    _OutboxRecord,
    _OutboxUsage,
    _RecoveredTopicPolicy,
)
from ._transport_delivery_policy import (
    DEFAULT_DELIVERY_ITEM_LIMIT,
    DEFAULT_DELIVERY_MAX_ATTEMPTS,
    DEFAULT_DELIVERY_SOFT_LIMIT_RATIO,
    DEFAULT_DELIVERY_STORAGE_BYTES,
    DeliveryConfig,
    DeliverySemantics,
    TopicDeliveryPolicy,
    _require_nonnegative_number,
    _require_optional_timeout,
    _require_positive_number,
    _require_text,
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

_WORK_BATCH_LIMIT = 32
_WORK_POLL_SECONDS = 0.05
_CLOSED_SENTINEL = object()


@final
class DurableDelivery:
    """SQLite-backed at-least-once delivery over one owned receive stream."""

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
            raise TypeError("observer must be callable")
        if receive_validator is not None and not callable(receive_validator):
            raise TypeError("receive_validator must be callable")
        if config.max_message_bytes + _DELIVERY_HEADER_SIZE + 256 > (
            transport.config.max_payload_bytes
        ):
            raise ValueError(
                "max_message_bytes plus delivery framing exceeds transport "
                "max_payload_bytes"
            )
        self.transport = transport
        self.config = config
        self._topic_policies = {
            policy.topic: policy for policy in config.topic_policies
        }
        self._owns_transport = owns_transport
        self._observer = observer
        self._receive_validator = receive_validator
        self._event_sequence = 0
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
        try:
            self._journal.validate_recovered_limits(
                topic_policies={
                    policy.topic: _RecoveredTopicPolicy(
                        policy.semantics.value,
                        policy.max_items,
                        policy.max_bytes,
                        policy.max_inbox_items,
                        policy.max_inbox_bytes,
                        latest_per_source=policy.latest_per_source,
                    )
                    for policy in config.topic_policies
                },
                default_topic_policy=_RecoveredTopicPolicy(
                    DeliverySemantics.APPEND.value,
                    config.max_outbox_items,
                    config.max_storage_bytes,
                    config.max_inbox_items,
                    config.max_storage_bytes,
                ),
            )
        except _JournalFull as error:
            self._journal.close()
            raise DeliveryStorageFull(str(error)) from error
        except _JournalError as error:
            self._journal.close()
            raise DeliveryError(
                f"could not validate recovered delivery journal: {error}"
            ) from error
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
        self._outbox_deduplicated = 0
        self._coalesced = 0
        self._soft_compactions = 0
        self._soft_watermark_crossings = 0
        self._storage_rejections = 0
        self._retry_exhausted = 0
        self._expired_outbox = 0
        self._expired_inbox = 0
        self._transport_backpressure_failures = 0
        self._transport_backpressure_streak = 0
        self._last_transport_connected_at: float | None = None
        self._last_error: str | None = None
        self._last_stats = self._journal.stats()
        self._recovered_outbox = self._last_stats.outbox_items
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
        if observer is not None:
            for record in self._journal.outbox_replay_records():
                self._emit(
                    DeliveryEventKind.REPLAYED,
                    record.message_id,
                    record.topic,
                    record.source_key,
                    correlation_id=record.correlation_id,
                    attempt=record.attempts,
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
        source: str | None = None,
    ) -> str:
        """Durably retain one append command or replaceable latest value."""
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
        policy = self._policy_for(message.channel)
        source_key = self._source_key(policy, source)
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
            detail = (
                "encoded durable message exceeds transport max_payload_bytes "
                f"({len(encoded)} > {self.transport.config.max_payload_bytes})"
            )
            self._emit(
                DeliveryEventKind.DROPPED,
                resolved_message_id,
                policy.topic,
                source_key,
                correlation_id=message.correlation_id,
                detail=detail,
            )
            raise ValueError(
                detail
            )
        resolved_ttl = policy.ttl_seconds
        if ttl_seconds is not None:
            requested_ttl = _require_positive_number(ttl_seconds, "ttl_seconds")
            if (
                message.channel in self._topic_policies
                and requested_ttl > policy.ttl_seconds
            ):
                raise ValueError(
                    f"ttl_seconds {requested_ttl} exceeds topic "
                    f"{policy.topic!r} limit {policy.ttl_seconds}"
                )
            resolved_ttl = requested_ttl
        now = time()
        record = _OutboxRecord(
            message_id=resolved_message_id,
            topic=policy.topic,
            semantics=policy.semantics.value,
            source_key=source_key,
            frame_kind=int(message.kind),
            channel=message.channel,
            correlation_id=message.correlation_id,
            payload=message.payload,
            attempts=0,
            max_attempts=policy.max_attempts,
        )
        try:
            result = self._journal.insert_outbox(
                record,
                created_at=now,
                expires_at=now + resolved_ttl,
                topic_item_limit=policy.max_items,
                topic_byte_limit=policy.max_bytes,
                soft_limit_ratio=policy.soft_limit_ratio,
            )
        except _JournalFull as error:
            self._change(storage_rejections=1, error=str(error))
            self._emit(
                DeliveryEventKind.DROPPED,
                resolved_message_id,
                policy.topic,
                source_key,
                correlation_id=message.correlation_id,
                detail=str(error),
                capacity=self._delivery_capacity(policy, error.capacity),
            )
            raise DeliveryStorageFull(str(error)) from error
        except _JournalConflict as error:
            self._emit(
                DeliveryEventKind.DROPPED,
                resolved_message_id,
                policy.topic,
                source_key,
                correlation_id=message.correlation_id,
                detail=str(error),
            )
            raise DeliveryConflict(str(error)) from error
        except _JournalError as error:
            raise DeliveryError(f"could not retain outbound message: {error}") from error
        if result.disposition is _OutboxDisposition.DEDUPLICATED:
            self._change(outbox_deduplicated=1)
            self._emit(
                DeliveryEventKind.DEDUPLICATED,
                resolved_message_id,
                policy.topic,
                source_key,
                correlation_id=message.correlation_id,
            )
        else:
            self._change(
                accepted=1,
                coalesced=(
                    1
                    if result.disposition is _OutboxDisposition.REPLACED
                    else 0
                ),
                expired_outbox=len(result.expired_outbox),
                soft_compactions=1 if result.soft_compaction else 0,
            )
            self._emit(
                (
                    DeliveryEventKind.COALESCED
                    if result.disposition is _OutboxDisposition.REPLACED
                    else DeliveryEventKind.ENQUEUED
                ),
                resolved_message_id,
                policy.topic,
                source_key,
                correlation_id=message.correlation_id,
                related_message_id=result.replaced_message_id,
            )
            if result.soft_compaction:
                self._emit(
                    DeliveryEventKind.SOFT_WATERMARK,
                    resolved_message_id,
                    policy.topic,
                    source_key,
                    correlation_id=message.correlation_id,
                    capacity=self._delivery_capacity(policy, result.capacity),
                )
            for expired in result.expired_outbox:
                self._emit(
                    DeliveryEventKind.EXPIRED,
                    expired.message_id,
                    expired.topic,
                    expired.source_key,
                    correlation_id=expired.correlation_id,
                    attempt=expired.attempts,
                    detail="soft watermark expiry sweep",
                )
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
                append_outbox_items=stats.append_outbox_items,
                latest_outbox_items=stats.latest_outbox_items,
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
                outbox_deduplicated=self._outbox_deduplicated,
                coalesced=self._coalesced,
                soft_compactions=self._soft_compactions,
                soft_watermark_crossings=self._soft_watermark_crossings,
                storage_rejections=self._storage_rejections,
                retry_exhausted=self._retry_exhausted,
                recovered_outbox=self._recovered_outbox,
                expired_outbox=self._expired_outbox,
                expired_inbox=self._expired_inbox,
                transport_backpressure_failures=(
                    self._transport_backpressure_failures
                ),
                transport_backpressure_streak=self._transport_backpressure_streak,
                last_error=self._last_error,
            )

    def topic_health(self, topic: str) -> DeliveryTopicHealth:
        """Return exact retained SQLite rows and bytes for one raw topic."""
        self._require_open()
        resolved_topic = _require_text(topic, "topic")
        stats = self._journal.topic_stats(resolved_topic)
        return DeliveryTopicHealth(
            topic=resolved_topic,
            retained_items=stats.outbox_items + stats.inbox_items,
            outbox_items=stats.outbox_items,
            append_outbox_items=stats.append_outbox_items,
            latest_outbox_items=stats.latest_outbox_items,
            inbox_items=stats.inbox_items,
            logical_storage_bytes=stats.logical_bytes,
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
                transport_health = self.transport.health()
                if transport_health.state is LinkState.CONNECTED:
                    if (
                        transport_health.connected_at is not None
                        and transport_health.connected_at
                        != self._last_transport_connected_at
                    ):
                        self._reset_transport_backpressure()
                        self._last_transport_connected_at = (
                            transport_health.connected_at
                        )
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
            try:
                frame = _encode_delivery_frame(
                    _DeliveryOperation.DATA,
                    record.message_id,
                    frame_kind=record.frame_kind,
                    channel=record.channel,
                    correlation_id=record.correlation_id,
                    payload=record.payload,
                    delivery_attempt=record.attempts + 1,
                )
                if len(frame) > self.transport.config.max_payload_bytes:
                    raise ValueError(
                        "encoded durable message exceeds transport "
                        "max_payload_bytes "
                        f"({len(frame)} > {self.transport.config.max_payload_bytes})"
                    )
            except ValueError as error:
                dropped = self._journal.delete_outbox(record.message_id)
                self._change(error=str(error))
                if dropped is not None:
                    self._emit(
                        DeliveryEventKind.DROPPED,
                        dropped.message_id,
                        dropped.topic,
                        dropped.source_key,
                        correlation_id=dropped.correlation_id,
                        attempt=dropped.attempts,
                        detail=str(error),
                    )
                continue
            try:
                self.transport.send(
                    TransportMessage(FrameKind.PUBSUB, DELIVERY_CHANNEL, frame),
                    timeout=0.01,
                )
            except (TransportClosed, TransportQueueFull) as error:
                pressure_count, delay = self._transport_backpressure_delay()
                retained = self._journal.mark_outbox_attempt(
                    record.message_id,
                    next_attempt_at=now + delay,
                    error=f"{type(error).__name__}: {error}",
                    increment_attempts=False,
                )
                self._change(error=f"{type(error).__name__}: {error}")
                if retained:
                    self._emit(
                        DeliveryEventKind.RETRY_SCHEDULED,
                        record.message_id,
                        record.topic,
                        record.source_key,
                        correlation_id=record.correlation_id,
                        attempt=record.attempts + 1,
                        detail=(
                            "local transport backpressure: "
                            f"{type(error).__name__}: {error}; "
                            f"pressure_count={pressure_count}; "
                            f"retry in {delay:.3f}s"
                        ),
                        local_pressure_count=pressure_count,
                    )
                continue
            attempt = record.attempts + 1
            retained = self._journal.mark_outbox_attempt(
                record.message_id,
                next_attempt_at=now + self._retry_delay(attempt),
                error=None,
                increment_attempts=True,
            )
            self._reset_transport_backpressure()
            self._change(frames_sent=1, retries=1 if record.attempts else 0)
            self._emit(
                DeliveryEventKind.SENT,
                record.message_id,
                record.topic,
                record.source_key,
                correlation_id=record.correlation_id,
                attempt=attempt,
            )
            if retained and attempt < record.max_attempts:
                self._emit(
                    DeliveryEventKind.RETRY_SCHEDULED,
                    record.message_id,
                    record.topic,
                    record.source_key,
                    correlation_id=record.correlation_id,
                    attempt=attempt + 1,
                    detail="awaiting acknowledgement",
                )

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
            if deleted is not None:
                self._change()
                self._emit(
                    DeliveryEventKind.ACKNOWLEDGED,
                    deleted.message_id,
                    deleted.topic,
                    deleted.source_key,
                    correlation_id=deleted.correlation_id,
                    attempt=deleted.attempts,
                )
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
        policy = self._policy_for(frame.channel)
        received_message = TransportMessage(
            FrameKind(frame.frame_kind),
            frame.channel,
            frame.payload,
            correlation_id=frame.correlation_id,
        )
        validator = self._receive_validator
        if validator is not None:
            try:
                validator(received_message)
            except Exception as error:
                self._send_control(
                    _DeliveryOperation.NACK,
                    frame.message_id,
                    self._bounded_control_reason(error),
                )
                self._change(storage_rejections=1, error=str(error))
                self._emit(
                    DeliveryEventKind.DROPPED,
                    frame.message_id,
                    frame.channel,
                    None,
                    correlation_id=frame.correlation_id,
                    attempt=frame.delivery_attempt,
                    detail=str(error),
                    capacity=self._inbox_delivery_capacity(policy, None),
                )
                return
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
                topic_item_limit=policy.max_inbox_items,
                topic_byte_limit=policy.max_inbox_bytes,
            )
        except _JournalFull as error:
            self._send_control(
                _DeliveryOperation.NACK,
                frame.message_id,
                self._bounded_control_reason(error),
            )
            self._change(storage_rejections=1, error=str(error))
            self._emit(
                DeliveryEventKind.DROPPED,
                frame.message_id,
                frame.channel,
                None,
                correlation_id=frame.correlation_id,
                attempt=frame.delivery_attempt,
                detail=str(error),
                capacity=self._inbox_delivery_capacity(policy, error.capacity),
            )
            return
        except _JournalConflict as error:
            self._send_control(
                _DeliveryOperation.NACK,
                frame.message_id,
                self._bounded_control_reason(error),
            )
            raise DeliveryConflict(str(error)) from error
        if disposition is _InboxDisposition.ACKED_DUPLICATE:
            self._journal.schedule_ack_now(frame.message_id, now)
            self._wake_sender.set()
            self._change(duplicates_suppressed=1)
            self._emit(
                DeliveryEventKind.DUPLICATE_SUPPRESSED,
                frame.message_id,
                frame.channel,
                None,
                correlation_id=frame.correlation_id,
                attempt=frame.delivery_attempt,
                detail="already acknowledged",
            )
            return
        if disposition is _InboxDisposition.PENDING_DUPLICATE:
            self._change(duplicates_suppressed=1)
            self._emit(
                DeliveryEventKind.DUPLICATE_SUPPRESSED,
                frame.message_id,
                frame.channel,
                None,
                correlation_id=frame.correlation_id,
                attempt=frame.delivery_attempt,
                detail="already pending",
            )
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
        self._reset_transport_backpressure()
        self._change(frames_sent=1)
        return True

    def _bounded_control_reason(self, error: BaseException) -> bytes:
        return str(error).encode("utf-8", errors="replace")[:_MAX_TEXT_BYTES]

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
        expired_outbox = len(result.expired_outbox)
        retry_exhausted = len(result.exhausted_outbox)
        expired_inbox = len(result.expired_inbox)
        if expired_inbox:
            self._purge_expired_inbox(
                tuple(expired.message_id for expired in result.expired_inbox)
            )
        if expired_outbox or retry_exhausted or expired_inbox:
            self._change(
                expired_outbox=expired_outbox,
                retry_exhausted=retry_exhausted,
                expired_inbox=expired_inbox,
                error=(
                    f"compacted {expired_outbox} expired outbox, "
                    f"{retry_exhausted} retry-exhausted outbox, and "
                    f"{expired_inbox} expired inbox records"
                ),
            )
        for expired in result.expired_outbox:
            self._emit(
                DeliveryEventKind.EXPIRED,
                expired.message_id,
                expired.topic,
                expired.source_key,
                correlation_id=expired.correlation_id,
                attempt=expired.attempts,
            )
        for exhausted in result.exhausted_outbox:
            self._emit(
                DeliveryEventKind.DROPPED,
                exhausted.message_id,
                exhausted.topic,
                exhausted.source_key,
                correlation_id=exhausted.correlation_id,
                attempt=exhausted.attempts,
                detail="retry budget exhausted",
            )
        for expired in result.expired_inbox:
            self._emit(
                DeliveryEventKind.EXPIRED,
                expired.message_id,
                expired.topic,
                None,
                correlation_id=expired.correlation_id,
                attempt=expired.delivery_attempt,
                detail="inbox retention expired",
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

    def _emit(
        self,
        kind: DeliveryEventKind,
        message_id: str,
        topic: str,
        source: str | None,
        *,
        correlation_id: str | None = None,
        attempt: int = 0,
        related_message_id: str | None = None,
        detail: str | None = None,
        capacity: DeliveryCapacity | None = None,
        local_pressure_count: int = 0,
    ) -> None:
        observer = self._observer
        if observer is None:
            return
        with self._condition:
            self._event_sequence += 1
            event = DeliveryEvent(
                sequence=self._event_sequence,
                occurred_at=time(),
                kind=kind,
                message_id=message_id,
                topic=topic,
                source=source or None,
                correlation_id=correlation_id,
                attempt=attempt,
                related_message_id=related_message_id,
                detail=detail,
                capacity=capacity,
                local_pressure_count=local_pressure_count,
            )
        try:
            observer(event)
        except Exception as error:
            with self._condition:
                self._last_error = (
                    f"delivery observer failed for event {event.sequence}: "
                    f"{type(error).__name__}: {error}"
                )
                self._generation += 1
                self._condition.notify_all()

    def _delivery_capacity(
        self,
        policy: TopicDeliveryPolicy,
        usage: _OutboxUsage | None,
    ) -> DeliveryCapacity | None:
        if usage is None:
            return None
        return DeliveryCapacity(
            peer_items=usage.items,
            peer_item_limit=self.config.max_outbox_items,
            peer_logical_bytes=usage.logical_bytes,
            peer_byte_limit=self.config.max_storage_bytes,
            topic_items=usage.topic_items,
            topic_item_limit=policy.max_items,
            topic_bytes=usage.topic_bytes,
            topic_byte_limit=policy.max_bytes,
            soft_limit_ratio=policy.soft_limit_ratio,
        )

    def _inbox_delivery_capacity(
        self,
        policy: TopicDeliveryPolicy,
        usage: _OutboxUsage | None,
    ) -> DeliveryCapacity | None:
        if usage is None:
            try:
                usage = self._journal.inbox_usage(policy.topic)
            except _JournalError:
                return None
        return DeliveryCapacity(
            peer_items=usage.items,
            peer_item_limit=self.config.max_inbox_items,
            peer_logical_bytes=usage.logical_bytes,
            peer_byte_limit=self.config.max_storage_bytes,
            topic_items=usage.topic_items,
            topic_item_limit=policy.max_inbox_items,
            topic_bytes=usage.topic_bytes,
            topic_byte_limit=policy.max_inbox_bytes,
            soft_limit_ratio=policy.soft_limit_ratio,
        )

    def _policy_for(self, topic: str) -> TopicDeliveryPolicy:
        configured = self._topic_policies.get(topic)
        if configured is not None:
            return configured
        return TopicDeliveryPolicy.commands(
            topic,
            max_items=self.config.max_outbox_items,
            max_bytes=self.config.max_storage_bytes,
            ttl_seconds=self.config.message_ttl_seconds,
            max_attempts=self.config.max_delivery_attempts,
            soft_limit_ratio=self.config.soft_limit_ratio,
        )

    def _source_key(
        self,
        policy: TopicDeliveryPolicy,
        source: str | None,
    ) -> str | None:
        if policy.semantics is DeliverySemantics.APPEND:
            if source is not None:
                raise ValueError("source is only valid for latest topic policies")
            return None
        if not policy.latest_per_source:
            if source is not None:
                raise ValueError(
                    f"latest topic {policy.topic!r} has one shared slot"
                )
            return ""
        if source is None:
            raise ValueError(
                f"latest topic {policy.topic!r} requires a source"
            )
        source = _require_text(source, "source")
        if len(source.encode("utf-8")) > _MAX_TEXT_BYTES:
            raise ValueError("encoded source is too long")
        return source

    def _retry_delay(self, attempt: int) -> float:
        try:
            delay = self.config.retry_initial_seconds * (
                self.config.retry_multiplier ** max(attempt - 1, 0)
            )
        except OverflowError:
            return self.config.retry_max_seconds
        return min(delay, self.config.retry_max_seconds)

    def _transport_backpressure_delay(self) -> tuple[int, float]:
        with self._condition:
            self._transport_backpressure_failures += 1
            self._transport_backpressure_streak += 1
            pressure_count = self._transport_backpressure_streak
        return pressure_count, self._retry_delay(pressure_count)

    def _reset_transport_backpressure(self) -> None:
        with self._condition:
            self._transport_backpressure_streak = 0

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
        outbox_deduplicated: int = 0,
        coalesced: int = 0,
        soft_compactions: int = 0,
        storage_rejections: int = 0,
        retry_exhausted: int = 0,
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
            self._outbox_deduplicated += outbox_deduplicated
            self._coalesced += coalesced
            self._soft_compactions += soft_compactions
            self._soft_watermark_crossings += soft_compactions
            self._storage_rejections += storage_rejections
            self._retry_exhausted += retry_exhausted
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


__all__ = [
    "DEFAULT_DELIVERY_ITEM_LIMIT",
    "DEFAULT_DELIVERY_MAX_ATTEMPTS",
    "DEFAULT_DELIVERY_SOFT_LIMIT_RATIO",
    "DEFAULT_DELIVERY_STORAGE_BYTES",
    "DELIVERY_CHANNEL",
    "DELIVERY_PROTOCOL_VERSION",
    "DeliveryCapacity",
    "DeliveryClosed",
    "DeliveryConfig",
    "DeliveryConflict",
    "DeliveryError",
    "DeliveryEvent",
    "DeliveryEventKind",
    "DeliveryHealth",
    "DeliveryObserver",
    "DeliveryProtocolError",
    "DeliveryReceiveValidator",
    "DeliverySemantics",
    "DeliveryStorageFull",
    "DeliveryTopicHealth",
    "DurableDelivery",
    "ReceivedDelivery",
    "TopicDeliveryPolicy",
]
