"""Single receive-loop owner for bounded durable delivery."""

from __future__ import annotations

from collections import deque
from threading import Condition, Lock, Thread
from time import monotonic, time
from typing import final

from ._transport_delivery_events import (
    _MAX_DELIVERY_OUTCOME_REASON_BYTES,
    DeliveryCapacity,
    DeliveryClosed,
    DeliveryConflict,
    DeliveryEventKind,
    DeliveryOutcome,
    DeliveryOutcomeKind,
    DeliveryReceiveValidator,
    DeliveryStore,
    ReceivedDelivery,
)
from ._transport_delivery_journal import (
    _DeliveryJournal,
)
from ._transport_delivery_journal_errors import _JournalConflict, _JournalFull
from ._transport_delivery_lifecycle import (
    _emit_committed_compaction,
    _emit_committed_watermark,
)
from ._transport_delivery_outcomes import _TerminalRejectionDisposition
from ._transport_delivery_policy import (
    DeliveryConfig,
    _is_volatile_delivery_topic,
)
from ._transport_delivery_protocol import (
    _decode_delivery_frame,
    _DeliveryFrame,
    _DeliveryOperation,
)
from ._transport_delivery_records import (
    _CompactionResult,
    _InboxDisposition,
    _InboxRecord,
    _ReplayCursor,
)
from ._transport_delivery_runtime import _DeliveryRuntime
from ._transport_delivery_sender import _DeliverySender
from .transport import (
    FrameKind,
    TcpTransport,
    TransportClosed,
    TransportMessage,
)

_RECEIVE_POLL_SECONDS = 0.05
_COMPACTION_INTERVAL_SECONDS = 0.1


def _bounded_outcome_reason(reason: str) -> str:
    normalized = reason.strip() or "delivery rejected"
    encoded = normalized.encode("utf-8")
    if len(encoded) <= _MAX_DELIVERY_OUTCOME_REASON_BYTES:
        return normalized
    suffix = "…".encode()
    prefix = encoded[
        : _MAX_DELIVERY_OUTCOME_REASON_BYTES - len(suffix)
    ].decode("utf-8", errors="ignore")
    return prefix + "…"


@final
class _DeliveryReceiver:
    """Own ``TcpTransport.receive`` and bounded application handoff state."""

    def __init__(
        self,
        transport: TcpTransport,
        config: DeliveryConfig,
        journal: _DeliveryJournal,
        runtime: _DeliveryRuntime,
        sender: _DeliverySender,
        validator: DeliveryReceiveValidator | None,
    ) -> None:
        self._transport = transport
        self._config = config
        self._journal = journal
        self._runtime = runtime
        self._sender = sender
        self._validator = validator
        self._condition = Condition(Lock())
        self._hydrate_lock = Lock()
        self._queue: deque[ReceivedDelivery] = deque()
        self._queued_ids: set[str] = set()
        self._inflight: dict[str, ReceivedDelivery] = {}
        self._replay_cursor: _ReplayCursor | None = None
        self._thread = Thread(
            target=self._run,
            name=(
                "manyfold-delivery-"
                f"{transport.identity.node_id}-receiver"
            ),
        )

    def start(self) -> None:
        self._hydrate_available()
        self._thread.start()

    def join(self, timeout: float) -> bool:
        self._thread.join(timeout=timeout)
        return not self._thread.is_alive()

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def dispose(self) -> None:
        if self._thread.is_alive():
            raise RuntimeError("cannot dispose a live delivery receiver")
        self.drain()
        self._validator = None

    def wake_receivers(self) -> None:
        with self._condition:
            self._condition.notify_all()

    def counts(self) -> tuple[int, int]:
        with self._condition:
            return len(self._queue), len(self._inflight)

    def receive(self, *, timeout: float | None) -> ReceivedDelivery:
        deadline = None if timeout is None else monotonic() + timeout
        while True:
            with self._condition:
                if self._runtime.is_closed():
                    raise DeliveryClosed("durable delivery is closed")
                while not self._queue:
                    if self._runtime.is_closed():
                        raise DeliveryClosed("durable delivery is closed")
                    remaining = (
                        None
                        if deadline is None
                        else max(0.0, deadline - monotonic())
                    )
                    if remaining == 0 or not self._condition.wait(
                        timeout=remaining
                    ):
                        raise TimeoutError(
                            "no durable delivery arrived before timeout"
                        )
                candidate = self._queue[0]
            with self._runtime.transition():
                if not self._journal.is_pending_inbox(
                    candidate.message_id,
                    time(),
                ):
                    self._compact_transition()
                    continue
                with self._condition:
                    if self._runtime.is_closed():
                        raise DeliveryClosed("durable delivery is closed")
                    if (
                        not self._queue
                        or self._queue[0].message_id != candidate.message_id
                    ):
                        continue
                    delivery = self._queue.popleft()
                    self._queued_ids.remove(delivery.message_id)
                    self._inflight[delivery.message_id] = delivery
                self._runtime.change(delivered=1)
                return delivery

    def ack(self, message_id: str) -> None:
        with self._runtime.transition():
            self._require_inflight(message_id)
            topic = self._topic_for_inflight(message_id)
            now = time()
            if not self._journal.mark_inbox_outcome(
                message_id,
                status="acked",
                reason=None,
                now=now,
                retention_seconds=self._config.dedupe_retention_seconds,
            ):
                raise DeliveryConflict(
                    f"inflight delivery {message_id!r} is no longer pending"
                )
            self._release_inflight(message_id)
            self._runtime.emit(
                DeliveryEventKind.ACKNOWLEDGED,
                message_id,
                topic,
                None,
                store=DeliveryStore.INBOX,
            )
            self._runtime.change(acknowledgements=1)
        self._sender.wake()
        self._hydrate_available()

    def nack(self, message_id: str, outcome: DeliveryOutcome) -> None:
        with self._runtime.transition():
            self._require_inflight(message_id)
            topic = self._topic_for_inflight(message_id)
            if outcome.kind is DeliveryOutcomeKind.RETRYABLE:
                if not self._journal.delete_retryable_inbox(message_id):
                    raise DeliveryConflict(
                        f"inflight delivery {message_id!r} is no longer pending"
                    )
                self._release_inflight(message_id)
                self._sender.enqueue_retryable(message_id, outcome.reason)
                self._runtime.emit(
                    DeliveryEventKind.RETRY_SCHEDULED,
                    message_id,
                    topic,
                    None,
                    store=DeliveryStore.INBOX,
                    outcome=outcome,
                )
                self._runtime.change(negative_acknowledgements=1)
                self._hydrate_available()
                return
            status = (
                "terminal"
                if outcome.kind is DeliveryOutcomeKind.TERMINAL
                else "expired"
            )
            if not self._journal.mark_inbox_outcome(
                message_id,
                status=status,
                reason=outcome.reason,
                now=time(),
                retention_seconds=self._config.dedupe_retention_seconds,
            ):
                raise DeliveryConflict(
                    f"inflight delivery {message_id!r} is no longer pending"
                )
            self._release_inflight(message_id)
            self._runtime.emit(
                (
                    DeliveryEventKind.DROPPED
                    if outcome.kind is DeliveryOutcomeKind.TERMINAL
                    else DeliveryEventKind.EXPIRED
                ),
                message_id,
                topic,
                None,
                store=DeliveryStore.INBOX,
                outcome=outcome,
            )
            self._runtime.change(
                negative_acknowledgements=1,
                terminal_drops=(
                    1 if outcome.kind is DeliveryOutcomeKind.TERMINAL else 0
                ),
                expired_inbox=(
                    1 if outcome.kind is DeliveryOutcomeKind.EXPIRED else 0
                ),
            )
        self._sender.wake()
        self._hydrate_available()

    def observe_committed_compaction(
        self,
        result: _CompactionResult,
        *,
        capacity: DeliveryCapacity | None = None,
        emit_empty: bool = False,
    ) -> None:
        self._purge(
            tuple(record.message_id for record in result.expired_inbox)
        )
        _emit_committed_compaction(
            self._runtime,
            result,
            capacity=capacity,
            emit_empty_sweep=emit_empty,
        )
        if result.expired_inbox:
            self._sender.wake()

    def drain(self) -> None:
        with self._condition:
            self._queue.clear()
            self._queued_ids.clear()
            self._inflight.clear()
            self._condition.notify_all()

    def _run(self) -> None:
        next_compaction = monotonic() + _COMPACTION_INTERVAL_SECONDS
        while not self._runtime.stop.is_set():
            try:
                message = self._transport.receive(
                    timeout=_RECEIVE_POLL_SECONDS
                )
            except TimeoutError:
                message = None
            except TransportClosed as error:
                if not self._runtime.stop.is_set():
                    self._runtime.fail(
                        f"{type(error).__name__}: {error}"
                    )
                break
            if message is not None:
                try:
                    frame = _decode_delivery_frame(
                        message,
                        max_message_bytes=self._config.max_message_bytes,
                    )
                    self._handle_frame(frame)
                except Exception as error:
                    self._runtime.fail(
                        f"{type(error).__name__}: {error}"
                    )
                    break
            if monotonic() >= next_compaction:
                try:
                    self._compact()
                    self._hydrate_available()
                except Exception as error:
                    self._runtime.fail(
                        f"{type(error).__name__}: {error}"
                    )
                    break
                next_compaction = (
                    monotonic() + _COMPACTION_INTERVAL_SECONDS
                )

    def _handle_frame(self, frame: _DeliveryFrame) -> None:
        if frame.operation is _DeliveryOperation.DATA:
            self._handle_data(frame)
        elif frame.operation is _DeliveryOperation.ACK:
            self._sender.handle_peer_ack(frame.message_id)
        elif frame.operation is _DeliveryOperation.NACK:
            if frame.outcome is None:
                raise ValueError("delivery NACK is missing its outcome")
            self._sender.handle_peer_outcome(frame.message_id, frame.outcome)
        elif frame.operation is _DeliveryOperation.CONFIRM:
            self._sender.handle_peer_confirm(frame.message_id)

    def _handle_data(self, frame: _DeliveryFrame) -> None:
        with self._runtime.transition():
            self._handle_data_transition(frame)

    def _handle_data_transition(self, frame: _DeliveryFrame) -> None:
        record = _InboxRecord(
            frame.message_id,
            frame.frame_kind,
            frame.channel,
            frame.correlation_id,
            frame.payload,
            frame.delivery_attempt,
        )
        if _is_volatile_delivery_topic(frame.channel):
            outcome = DeliveryOutcome.terminal(
                "volatile delivery channels cannot be durable"
            )
            self._sender.enqueue_terminal(record.message_id, outcome.reason)
            self._runtime.emit(
                DeliveryEventKind.DROPPED,
                record.message_id,
                record.channel,
                None,
                store=DeliveryStore.INBOX,
                correlation_id=record.correlation_id,
                attempt=record.delivery_attempt,
                outcome=outcome,
            )
            self._runtime.change(terminal_drops=1)
            return
        try:
            policy = self._config.policy_for(frame.channel)
        except ValueError as error:
            self._persist_terminal_rejection(record, str(error))
            return
        now = time()
        try:
            inserted = self._journal.record_inbox(
                record,
                created_at=now,
                expires_at=now + policy.ttl_seconds,
                now=now,
                policy=policy,
            )
        except _JournalFull as error:
            self._sender.enqueue_retryable(frame.message_id, str(error))
            self._runtime.change(storage_rejections=1, error=str(error))
            return
        except _JournalConflict as error:
            outcome = DeliveryOutcome.terminal(
                _bounded_outcome_reason(str(error))
            )
            self._sender.enqueue_terminal(
                record.message_id,
                outcome.reason,
            )
            self._runtime.emit(
                DeliveryEventKind.DROPPED,
                record.message_id,
                record.channel,
                None,
                store=DeliveryStore.INBOX,
                correlation_id=record.correlation_id,
                attempt=record.delivery_attempt,
                outcome=outcome,
            )
            self._runtime.change(terminal_drops=1)
            return
        if inserted.crossing is not None:
            _emit_committed_watermark(
                self._runtime,
                topic=frame.channel,
                crossing=inserted.crossing,
                store=DeliveryStore.INBOX,
            )
        if inserted.sweep is not None:
            self.observe_committed_compaction(
                inserted.sweep,
                capacity=inserted.capacity,
                emit_empty=True,
            )
        if inserted.disposition is _InboxDisposition.NEW:
            self._runtime.emit(
                DeliveryEventKind.ENQUEUED,
                frame.message_id,
                frame.channel,
                None,
                store=DeliveryStore.INBOX,
                correlation_id=frame.correlation_id,
                attempt=frame.delivery_attempt,
            )
            self._validate_and_queue(record)
            return
        self._runtime.emit(
            DeliveryEventKind.DUPLICATE_SUPPRESSED,
            frame.message_id,
            frame.channel,
            None,
            store=DeliveryStore.INBOX,
            correlation_id=frame.correlation_id,
            attempt=frame.delivery_attempt,
        )
        self._runtime.change(duplicates_suppressed=1)
        if inserted.disposition in {
            _InboxDisposition.ACKED_DUPLICATE,
            _InboxDisposition.TERMINAL_DUPLICATE,
            _InboxDisposition.EXPIRED_DUPLICATE,
        }:
            self._journal.schedule_response_now(
                frame.message_id,
                now,
                max_attempts=self._config.max_ack_attempts,
            )
            self._sender.wake()
        elif not self._is_held(frame.message_id):
            self._validate_and_queue(record)

    def _validate_and_queue(self, record: _InboxRecord) -> None:
        with self._runtime.transition():
            self._validate_and_queue_transition(record)

    def _validate_and_queue_transition(self, record: _InboxRecord) -> None:
        message = TransportMessage(
            FrameKind(record.frame_kind),
            record.channel,
            record.payload,
            record.correlation_id,
        )
        if self._validator is not None:
            try:
                with self._runtime.callback():
                    self._validator(message)
            except Exception as error:
                outcome = DeliveryOutcome.terminal(
                    _bounded_outcome_reason(
                        f"{type(error).__name__}: {error}"
                    )
                )
                if self._journal.mark_inbox_outcome(
                    record.message_id,
                    status="terminal",
                    reason=outcome.reason,
                    now=time(),
                    retention_seconds=self._config.dedupe_retention_seconds,
                ):
                    self._runtime.emit(
                        DeliveryEventKind.DROPPED,
                        record.message_id,
                        record.channel,
                        None,
                        store=DeliveryStore.INBOX,
                        correlation_id=record.correlation_id,
                        attempt=record.delivery_attempt,
                        outcome=outcome,
                    )
                    self._runtime.change(terminal_drops=1)
                    self._sender.wake()
                return
        delivery = ReceivedDelivery(
            record.message_id,
            message,
            record.delivery_attempt,
        )
        with self._condition:
            if (
                record.message_id in self._queued_ids
                or record.message_id in self._inflight
                or len(self._queue) + len(self._inflight)
                >= self._config.receive_queue_limit
            ):
                return
            self._queue.append(delivery)
            self._queued_ids.add(record.message_id)
            self._condition.notify()
        self._runtime.change()

    def _persist_terminal_rejection(
        self,
        record: _InboxRecord,
        reason: str,
    ) -> None:
        with self._runtime.transition():
            self._persist_terminal_rejection_transition(record, reason)

    def _persist_terminal_rejection_transition(
        self,
        record: _InboxRecord,
        reason: str,
    ) -> None:
        bounded_reason = _bounded_outcome_reason(reason)
        try:
            inserted = self._journal.record_terminal_rejection(
                record,
                reason=bounded_reason,
                now=time(),
            )
        except _JournalFull as error:
            self._sender.enqueue_retryable(record.message_id, str(error))
            self._runtime.change(storage_rejections=1, error=str(error))
            return
        if inserted.crossing is not None:
            _emit_committed_watermark(
                self._runtime,
                topic=record.channel,
                crossing=inserted.crossing,
                store=DeliveryStore.INBOX,
            )
        if inserted.sweep is not None:
            self.observe_committed_compaction(
                inserted.sweep,
                capacity=inserted.capacity,
                emit_empty=True,
            )
        if inserted.disposition is _TerminalRejectionDisposition.CONFLICT:
            outcome = DeliveryOutcome.terminal(
                "message_id conflicts with retained delivery content"
            )
            self._sender.enqueue_terminal(record.message_id, outcome.reason)
            self._runtime.emit(
                DeliveryEventKind.DROPPED,
                record.message_id,
                record.channel,
                None,
                store=DeliveryStore.INBOX,
                correlation_id=record.correlation_id,
                attempt=record.delivery_attempt,
                outcome=outcome,
            )
            self._runtime.change(terminal_drops=1)
            return
        if inserted.disposition is _TerminalRejectionDisposition.DUPLICATE:
            self._runtime.emit(
                DeliveryEventKind.DUPLICATE_SUPPRESSED,
                record.message_id,
                record.channel,
                None,
                store=DeliveryStore.INBOX,
                correlation_id=record.correlation_id,
                attempt=record.delivery_attempt,
                outcome=DeliveryOutcome.terminal(bounded_reason),
            )
            self._runtime.change(duplicates_suppressed=1)
            self._sender.wake()
            return
        self._purge((record.message_id,))
        outcome = DeliveryOutcome.terminal(bounded_reason)
        self._runtime.emit(
            DeliveryEventKind.DROPPED,
            record.message_id,
            record.channel,
            None,
            store=DeliveryStore.INBOX,
            correlation_id=record.correlation_id,
            attempt=record.delivery_attempt,
            outcome=outcome,
        )
        self._runtime.change(terminal_drops=1)
        self._sender.wake()

    def _hydrate_available(self) -> None:
        if self._runtime.stop.is_set():
            return
        with self._hydrate_lock:
            while True:
                with self._condition:
                    available = self._config.receive_queue_limit - (
                        len(self._queue) + len(self._inflight)
                    )
                if available <= 0:
                    return
                batch = self._journal.pending_inbox_batch(
                    time(),
                    self._replay_cursor,
                    limit=min(available, self._config.recovery_batch_size),
                )
                if not batch:
                    self._replay_cursor = None
                    return
                for record, cursor in batch:
                    self._replay_cursor = cursor
                    if not self._is_held(record.message_id):
                        self._validate_and_queue(record)

    def _compact(self) -> None:
        with self._runtime.transition():
            self._compact_transition()

    def _compact_transition(self) -> None:
        result = self._journal.compact(
            time(),
            limit=self._config.work_batch_size,
        )
        self.observe_committed_compaction(result)

    def _purge(self, message_ids: tuple[str, ...]) -> None:
        if not message_ids:
            return
        removed = set(message_ids)
        changed = False
        with self._condition:
            previous_queue_size = len(self._queue)
            self._queue = deque(
                delivery
                for delivery in self._queue
                if delivery.message_id not in removed
            )
            changed = len(self._queue) != previous_queue_size
            self._queued_ids.difference_update(removed)
            for message_id in removed:
                changed = (
                    self._inflight.pop(message_id, None) is not None
                    or changed
                )
            self._condition.notify_all()
        if changed:
            self._runtime.change()

    def _is_held(self, message_id: str) -> bool:
        with self._condition:
            return (
                message_id in self._queued_ids
                or message_id in self._inflight
            )

    def _require_inflight(self, message_id: str) -> None:
        with self._condition:
            if message_id not in self._inflight:
                raise DeliveryConflict(
                    f"message_id {message_id!r} is not awaiting application outcome"
                )

    def _release_inflight(self, message_id: str) -> None:
        with self._condition:
            del self._inflight[message_id]
            self._condition.notify_all()

    def _topic_for_inflight(self, message_id: str) -> str:
        with self._condition:
            delivery = self._inflight.get(message_id)
            if delivery is None:
                raise DeliveryConflict(
                    f"message_id {message_id!r} is not awaiting application outcome"
                )
            return delivery.message.channel
