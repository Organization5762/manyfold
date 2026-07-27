"""Single network-send owner for bounded durable delivery."""

from __future__ import annotations

from dataclasses import dataclass
from queue import Empty, Full, Queue
from threading import Thread
from time import monotonic, time
from typing import final

from ._transport_delivery_events import (
    DeliveryEventKind,
    DeliveryOutcome,
    DeliveryOutcomeKind,
    DeliveryStore,
)
from ._transport_delivery_journal import _DeliveryJournal
from ._transport_delivery_lifecycle import _emit_committed_compaction
from ._transport_delivery_policy import DeliveryConfig, _bounded_retry_delay
from ._transport_delivery_protocol import (
    DELIVERY_CHANNEL,
    _DeliveryOperation,
    _encode_delivery_frame,
)
from ._transport_delivery_records import _InboxRecord, _OutboxRecord
from ._transport_delivery_runtime import _DeliveryRuntime
from .transport import (
    FrameKind,
    LinkState,
    TcpTransport,
    TransportClosed,
    TransportMessage,
    TransportQueueFull,
)

_WORK_POLL_SECONDS = 0.02
_COMPACTION_INTERVAL_SECONDS = 0.1


def _response_frame(
    record: _InboxRecord,
) -> tuple[_DeliveryOperation, DeliveryOutcome | None]:
    if record.status == "acked":
        return _DeliveryOperation.ACK, None
    if record.status == "terminal":
        return (
            _DeliveryOperation.NACK,
            DeliveryOutcome.terminal(
                record.outcome_reason or "delivery rejected"
            ),
        )
    if record.status == "expired":
        return (
            _DeliveryOperation.NACK,
            DeliveryOutcome.expired(
                record.outcome_reason or "delivery expired"
            ),
        )
    raise ValueError(f"unsupported durable response status {record.status!r}")


@final
@dataclass(frozen=True, slots=True)
class _Control:
    operation: _DeliveryOperation
    message_id: str
    outcome: DeliveryOutcome | None = None


@final
class _DeliverySender:
    """Own every call to ``TcpTransport.send`` on one delivery endpoint."""

    def __init__(
        self,
        transport: TcpTransport,
        config: DeliveryConfig,
        journal: _DeliveryJournal,
        runtime: _DeliveryRuntime,
    ) -> None:
        self._transport = transport
        self._config = config
        self._journal = journal
        self._runtime = runtime
        self._controls: Queue[_Control] = Queue(
            maxsize=config.receive_queue_limit
        )
        self._thread = Thread(
            target=self._run,
            name=(
                "manyfold-delivery-"
                f"{transport.identity.node_id}-sender"
            ),
        )

    def start(self) -> None:
        self._thread.start()

    def join(self, timeout: float) -> bool:
        self._thread.join(timeout=timeout)
        return not self._thread.is_alive()

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def dispose(self) -> None:
        if self._thread.is_alive():
            raise RuntimeError("cannot dispose a live delivery sender")
        while True:
            try:
                self._controls.get_nowait()
            except Empty:
                return

    def wake(self) -> None:
        self._runtime.wake_sender.set()

    def enqueue_retryable(self, message_id: str, reason: str) -> None:
        self._enqueue_control(
            _Control(
                _DeliveryOperation.NACK,
                message_id,
                DeliveryOutcome.retryable(reason),
            )
        )

    def enqueue_terminal(self, message_id: str, reason: str) -> None:
        self._enqueue_control(
            _Control(
                _DeliveryOperation.NACK,
                message_id,
                DeliveryOutcome.terminal(reason),
            )
        )

    def handle_peer_ack(self, message_id: str) -> None:
        with self._runtime.transition():
            record = self._journal.delete_outbox(message_id)
            self._enqueue_control(
                _Control(_DeliveryOperation.CONFIRM, message_id)
            )
            if record is None:
                return
            self._runtime.emit(
                DeliveryEventKind.ACKNOWLEDGED,
                record.message_id,
                record.channel,
                record.source_key,
                store=DeliveryStore.OUTBOX,
                correlation_id=record.correlation_id,
                attempt=record.attempts,
            )
            self._runtime.change(peer_acknowledgements=1)

    def handle_peer_outcome(
        self,
        message_id: str,
        outcome: DeliveryOutcome,
    ) -> None:
        with self._runtime.transition():
            if outcome.kind is DeliveryOutcomeKind.RETRYABLE:
                record = self._journal.schedule_outbox_nack(
                    message_id,
                    now=time(),
                )
            else:
                record = self._journal.delete_outbox(message_id)
                self._enqueue_control(
                    _Control(_DeliveryOperation.CONFIRM, message_id)
                )
            if record is None:
                return
            self._runtime.change(peer_negative_acknowledgements=1)
            if outcome.kind is DeliveryOutcomeKind.RETRYABLE:
                self._runtime.emit(
                    DeliveryEventKind.RETRY_SCHEDULED,
                    message_id,
                    record.channel,
                    record.source_key,
                    store=DeliveryStore.OUTBOX,
                    correlation_id=record.correlation_id,
                    attempt=record.attempts,
                    outcome=outcome,
                )
                return
            kind = (
                DeliveryEventKind.EXPIRED
                if outcome.kind is DeliveryOutcomeKind.EXPIRED
                else DeliveryEventKind.DROPPED
            )
            self._runtime.emit(
                kind,
                record.message_id,
                record.channel,
                record.source_key,
                store=DeliveryStore.OUTBOX,
                correlation_id=record.correlation_id,
                attempt=record.attempts,
                outcome=outcome,
            )
            self._runtime.change(
                terminal_drops=(
                    1 if outcome.kind is DeliveryOutcomeKind.TERMINAL else 0
                ),
                expired_outbox=(
                    1 if outcome.kind is DeliveryOutcomeKind.EXPIRED else 0
                ),
            )

    def handle_peer_confirm(self, message_id: str) -> None:
        with self._runtime.transition():
            self._journal.confirm_response(message_id)

    def _run(self) -> None:
        next_compaction = monotonic() + _COMPACTION_INTERVAL_SECONDS
        while not self._runtime.stop.is_set():
            try:
                did_work = self._send_controls()
                if self._transport.health().state is LinkState.CONNECTED:
                    did_work = self._send_due_responses() or did_work
                    did_work = self._send_due_outbox() or did_work
                if monotonic() >= next_compaction:
                    with self._runtime.transition():
                        result = self._journal.compact_outbox(
                            time(),
                            limit=self._config.work_batch_size,
                        )
                        _emit_committed_compaction(self._runtime, result)
                    next_compaction = (
                        monotonic() + _COMPACTION_INTERVAL_SECONDS
                    )
            except Exception as error:
                self._runtime.fail(
                    f"{type(error).__name__}: {error}"
                )
                break
            if not did_work:
                self._runtime.wake_sender.wait(_WORK_POLL_SECONDS)
            self._runtime.wake_sender.clear()

    def _send_controls(self) -> bool:
        did_work = False
        for _ in range(self._config.work_batch_size):
            try:
                control = self._controls.get_nowait()
            except Empty:
                break
            did_work = True
            with self._runtime.transition():
                if self._runtime.stop.is_set():
                    break
                try:
                    self._send_encoded(
                        control.operation,
                        control.message_id,
                        outcome=control.outcome,
                    )
                except (TransportClosed, TransportQueueFull) as error:
                    self._runtime.change(
                        error=f"{type(error).__name__}: {error}"
                    )
                    break
                self._runtime.change(
                    frames_sent=1,
                    transport_backpressure_streak=0,
                )
        return did_work

    def _send_due_responses(self) -> bool:
        did_work = False
        with self._runtime.transition():
            now = time()
            records = self._journal.due_responses(
                now,
                limit=self._config.work_batch_size,
                max_attempts=self._config.max_ack_attempts,
            )
            for record in records:
                if self._runtime.stop.is_set():
                    break
                operation, outcome = _response_frame(record)
                try:
                    self._send_encoded(
                        operation,
                        record.message_id,
                        outcome=outcome,
                    )
                except TransportQueueFull:
                    self._record_response_pressure(record)
                    break
                except TransportClosed as error:
                    if not self._runtime.stop.is_set():
                        self._record_response_pressure(record, error=error)
                    break
                did_work = True
                next_attempt = record.ack_attempts + 1
                exhausted = self._journal.mark_response_attempt(
                    record.message_id,
                    next_attempt_at=now
                    + _bounded_retry_delay(self._config, next_attempt),
                    max_attempts=self._config.max_ack_attempts,
                )
                self._runtime.change(
                    frames_sent=1,
                    ack_retry_exhausted=1 if exhausted else 0,
                    transport_backpressure_streak=0,
                )
        return did_work

    def _send_due_outbox(self) -> bool:
        did_work = False
        with self._runtime.transition():
            now = time()
            records = self._journal.due_outbox(
                now,
                limit=self._config.work_batch_size,
            )
            for record in records:
                if self._runtime.stop.is_set():
                    break
                attempt = record.attempts + 1
                try:
                    self._send_data(record, attempt)
                except TransportQueueFull:
                    self._record_outbox_pressure(record)
                    break
                except TransportClosed as error:
                    if not self._runtime.stop.is_set():
                        self._record_outbox_pressure(record, error=error)
                    break
                did_work = True
                marked = self._journal.mark_outbox_sent(
                    record.message_id,
                    next_attempt_at=now
                    + _bounded_retry_delay(self._config, attempt),
                )
                if not marked:
                    continue
                self._runtime.emit(
                    DeliveryEventKind.SENT,
                    record.message_id,
                    record.channel,
                    record.source_key,
                    store=DeliveryStore.OUTBOX,
                    correlation_id=record.correlation_id,
                    attempt=attempt,
                )
                self._runtime.change(
                    frames_sent=1,
                    retries=1 if attempt > 1 else 0,
                    transport_backpressure_streak=0,
                )
        return did_work

    def _send_data(self, record: _OutboxRecord, attempt: int) -> None:
        payload = _encode_delivery_frame(
            _DeliveryOperation.DATA,
            record.message_id,
            frame_kind=record.frame_kind,
            channel=record.channel,
            correlation_id=record.correlation_id,
            payload=record.payload,
            delivery_attempt=attempt,
        )
        self._transport.send(
            TransportMessage(FrameKind.PUBSUB, DELIVERY_CHANNEL, payload)
        )

    def _send_encoded(
        self,
        operation: _DeliveryOperation,
        message_id: str,
        *,
        outcome: DeliveryOutcome | None = None,
    ) -> None:
        payload = _encode_delivery_frame(
            operation,
            message_id,
            outcome=outcome,
        )
        self._transport.send(
            TransportMessage(FrameKind.PUBSUB, DELIVERY_CHANNEL, payload)
        )

    def _record_outbox_pressure(
        self,
        record: _OutboxRecord,
        *,
        error: Exception | None = None,
    ) -> None:
        health = self._runtime.health(
            self._journal.stats(),
            queued_deliveries=0,
            inflight_deliveries=0,
        )
        streak = health.transport_backpressure_streak + 1
        delay = _bounded_retry_delay(self._config, streak)
        self._journal.schedule_outbox_retry(
            record.message_id,
            next_attempt_at=time() + delay,
        )
        self._runtime.emit(
            DeliveryEventKind.RETRY_SCHEDULED,
            record.message_id,
            record.channel,
            record.source_key,
            store=DeliveryStore.OUTBOX,
            correlation_id=record.correlation_id,
            attempt=record.attempts,
            local_pressure_count=streak,
        )
        self._runtime.change(
            transport_backpressure_failures=1,
            transport_backpressure_streak=streak,
            error=(
                None
                if error is None
                else f"{type(error).__name__}: {error}"
            ),
        )

    def _record_response_pressure(
        self,
        record: _InboxRecord,
        *,
        error: Exception | None = None,
    ) -> None:
        health = self._runtime.health(
            self._journal.stats(),
            queued_deliveries=0,
            inflight_deliveries=0,
        )
        streak = health.transport_backpressure_streak + 1
        self._journal.delay_response(
            record.message_id,
            next_attempt_at=time()
            + _bounded_retry_delay(self._config, streak),
            max_attempts=self._config.max_ack_attempts,
        )
        self._runtime.change(
            transport_backpressure_failures=1,
            transport_backpressure_streak=streak,
            error=(
                None
                if error is None
                else f"{type(error).__name__}: {error}"
            ),
        )

    def _enqueue_control(self, control: _Control) -> None:
        try:
            self._controls.put_nowait(control)
        except Full:
            self._runtime.change(
                error=(
                    "bounded delivery control queue is full; "
                    "the durable peer will retry"
                )
            )
            return
        self.wake()
