"""Shared bounded state for delivery sender and receiver components."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Condition, Event, Lock, RLock, local
from time import monotonic, time
from typing import final

from ._transport_delivery_events import (
    DeliveryCapacity,
    DeliveryCapacityDimension,
    DeliveryClosed,
    DeliveryError,
    DeliveryEvent,
    DeliveryEventKind,
    DeliveryHealth,
    DeliveryObserver,
    DeliveryOutcome,
    DeliveryStore,
)
from ._transport_delivery_records import _JournalStats

# Keeps callback and transport diagnostic graphs bounded independently of the
# smaller wire outcome-reason contract. The newest detail replaces the prior one.
_MAX_DELIVERY_ERROR_BYTES = 4096


def _bounded_error_detail(value: str) -> str:
    encoded = value.encode("utf-8", errors="replace")
    if len(encoded) <= _MAX_DELIVERY_ERROR_BYTES:
        return value
    suffix = "…".encode()
    prefix = encoded[
        : _MAX_DELIVERY_ERROR_BYTES - len(suffix)
    ].decode("utf-8", errors="ignore")
    return prefix + "…"


@final
@dataclass(slots=True)
class _Counters:
    accepted: int = 0
    frames_sent: int = 0
    retries: int = 0
    delivered: int = 0
    acknowledgements: int = 0
    negative_acknowledgements: int = 0
    peer_acknowledgements: int = 0
    peer_negative_acknowledgements: int = 0
    duplicates_suppressed: int = 0
    outbox_deduplicated: int = 0
    coalesced: int = 0
    watermark_crossings: int = 0
    expiry_sweeps: int = 0
    sweep_deleted_rows: int = 0
    recovered_watermarks: int = 0
    storage_rejections: int = 0
    terminal_drops: int = 0
    retry_exhausted: int = 0
    ack_retry_exhausted: int = 0
    recovered_outbox: int = 0
    expired_outbox: int = 0
    expired_inbox: int = 0
    transport_backpressure_failures: int = 0
    transport_backpressure_streak: int = 0


@final
class _DeliveryRuntime:
    def __init__(self, observer: DeliveryObserver | None) -> None:
        self.condition = Condition(Lock())
        self._observer_lock = RLock()
        self._transition_lock = RLock()
        self._transition_condition = Condition(Lock())
        self._transition_waiters = 0
        self._callback_context = local()
        self.stop = Event()
        self.wake_sender = Event()
        self._observer = observer
        self._counters = _Counters()
        self._generation = 0
        self._event_sequence = 0
        self._closed = False
        self._closing = False
        self._active_operations = 0
        self._last_error: str | None = None
        self._unavailable_waker: Callable[[], None] | None = None

    def begin_close(self) -> bool:
        with self.condition:
            if self._closed or self._closing:
                return False
            self._closing = True
            self._generation += 1
            self.condition.notify_all()
        return True

    def stop_workers(self) -> None:
        self.stop.set()
        self.wake_sender.set()

    def start_close(self) -> bool:
        started = self.begin_close()
        self.stop_workers()
        with self.condition:
            self.condition.notify_all()
        return started

    def set_unavailable_waker(self, waker: Callable[[], None]) -> None:
        with self.condition:
            self._unavailable_waker = waker

    def fail(self, error: str) -> None:
        self.change(error=error)
        self.start_close()
        with self.condition:
            waker = self._unavailable_waker
        if waker is not None:
            waker()

    @contextmanager
    def operation(self) -> Iterator[None]:
        with self.condition:
            if self._closed or self._closing:
                raise DeliveryClosed("durable delivery is closed")
            self._active_operations += 1
        try:
            yield
        finally:
            with self.condition:
                self._active_operations -= 1
                self.condition.notify_all()

    def wait_for_operations(self, *, timeout: float) -> bool:
        deadline = monotonic() + timeout
        with self.condition:
            while self._active_operations:
                remaining = max(0.0, deadline - monotonic())
                if remaining == 0 or not self.condition.wait(
                    timeout=remaining
                ):
                    return False
        return True

    def finish_close(self) -> None:
        with self.condition:
            self._closing = False
            self._closed = True
            self._generation += 1
            self.condition.notify_all()

    def is_closed(self) -> bool:
        with self.condition:
            return self._closed or self._closing

    def dispose_observer(self) -> None:
        with self._observer_lock:
            self._observer = None
        with self.condition:
            self._unavailable_waker = None

    @contextmanager
    def transition(self) -> Iterator[None]:
        with self._transition_condition:
            self._transition_waiters += 1
            self._transition_condition.notify_all()
        is_waiting = True
        try:
            with self._transition_lock:
                with self._transition_condition:
                    self._transition_waiters -= 1
                    is_waiting = False
                    self._transition_condition.notify_all()
                yield
        finally:
            if is_waiting:
                with self._transition_condition:
                    self._transition_waiters -= 1
                    self._transition_condition.notify_all()

    def wait_for_transition_waiters(
        self,
        minimum: int,
        *,
        timeout: float,
    ) -> bool:
        deadline = monotonic() + timeout
        with self._transition_condition:
            while self._transition_waiters < minimum:
                remaining = max(0.0, deadline - monotonic())
                if remaining == 0 or not self._transition_condition.wait(
                    timeout=remaining
                ):
                    return False
            return True

    def require_callback_read_only(self) -> None:
        if getattr(self._callback_context, "active", False):
            raise DeliveryError(
                "delivery observer and validator callbacks may call only "
                "read-only health APIs"
            )

    @contextmanager
    def callback(self) -> Iterator[None]:
        callback_was_active = getattr(
            self._callback_context,
            "active",
            False,
        )
        try:
            self._callback_context.active = True
            yield
        finally:
            self._callback_context.active = callback_was_active

    def wait_for_change(
        self,
        after_generation: int,
        *,
        timeout: float | None,
        allow_closing: bool = False,
    ) -> None:
        deadline = None if timeout is None else monotonic() + timeout
        with self.condition:
            while self._generation <= after_generation:
                if self._closed or (self._closing and not allow_closing):
                    raise DeliveryClosed(
                        "durable delivery closed before health changed"
                    )
                remaining = (
                    None if deadline is None else max(0.0, deadline - monotonic())
                )
                if remaining == 0 or not self.condition.wait(timeout=remaining):
                    raise TimeoutError(
                        "delivery health did not change before timeout"
                    )

    def change(
        self,
        *,
        accepted: int = 0,
        frames_sent: int = 0,
        retries: int = 0,
        delivered: int = 0,
        acknowledgements: int = 0,
        negative_acknowledgements: int = 0,
        peer_acknowledgements: int = 0,
        peer_negative_acknowledgements: int = 0,
        duplicates_suppressed: int = 0,
        outbox_deduplicated: int = 0,
        coalesced: int = 0,
        watermark_crossings: int = 0,
        expiry_sweeps: int = 0,
        sweep_deleted_rows: int = 0,
        recovered_watermarks: int = 0,
        storage_rejections: int = 0,
        terminal_drops: int = 0,
        retry_exhausted: int = 0,
        ack_retry_exhausted: int = 0,
        recovered_outbox: int = 0,
        expired_outbox: int = 0,
        expired_inbox: int = 0,
        transport_backpressure_failures: int = 0,
        transport_backpressure_streak: int | None = None,
        error: str | None = None,
    ) -> None:
        with self.condition:
            counters = self._counters
            counters.accepted += accepted
            counters.frames_sent += frames_sent
            counters.retries += retries
            counters.delivered += delivered
            counters.acknowledgements += acknowledgements
            counters.negative_acknowledgements += negative_acknowledgements
            counters.peer_acknowledgements += peer_acknowledgements
            counters.peer_negative_acknowledgements += (
                peer_negative_acknowledgements
            )
            counters.duplicates_suppressed += duplicates_suppressed
            counters.outbox_deduplicated += outbox_deduplicated
            counters.coalesced += coalesced
            counters.watermark_crossings += watermark_crossings
            counters.expiry_sweeps += expiry_sweeps
            counters.sweep_deleted_rows += sweep_deleted_rows
            counters.recovered_watermarks += recovered_watermarks
            counters.storage_rejections += storage_rejections
            counters.terminal_drops += terminal_drops
            counters.retry_exhausted += retry_exhausted
            counters.ack_retry_exhausted += ack_retry_exhausted
            counters.recovered_outbox += recovered_outbox
            counters.expired_outbox += expired_outbox
            counters.expired_inbox += expired_inbox
            counters.transport_backpressure_failures += (
                transport_backpressure_failures
            )
            if transport_backpressure_streak is not None:
                counters.transport_backpressure_streak = (
                    transport_backpressure_streak
                )
            if error is not None:
                self._last_error = _bounded_error_detail(error)
            self._generation += 1
            self.condition.notify_all()

    def emit(
        self,
        kind: DeliveryEventKind,
        message_id: str | None,
        topic: str | None,
        source: str | None,
        *,
        store: DeliveryStore | None = None,
        capacity_dimension: DeliveryCapacityDimension | None = None,
        correlation_id: str | None = None,
        attempt: int = 0,
        related_message_id: str | None = None,
        outcome: DeliveryOutcome | None = None,
        capacity: DeliveryCapacity | None = None,
        local_pressure_count: int = 0,
        affected_items: int = 0,
        deleted_items: int = 0,
        released_logical_bytes: int = 0,
    ) -> DeliveryEvent:
        with self._observer_lock:
            with self.condition:
                self._event_sequence += 1
                event = DeliveryEvent(
                    sequence=self._event_sequence,
                    occurred_at=time(),
                    kind=kind,
                    message_id=message_id,
                    topic=topic,
                    source=source,
                    store=store,
                    capacity_dimension=capacity_dimension,
                    correlation_id=correlation_id,
                    attempt=attempt,
                    related_message_id=related_message_id,
                    outcome=outcome,
                    capacity=capacity,
                    local_pressure_count=local_pressure_count,
                    affected_items=affected_items,
                    deleted_items=deleted_items,
                    released_logical_bytes=released_logical_bytes,
                )
            if self._observer is not None:
                try:
                    with self.callback():
                        self._observer(event)
                except Exception as error:
                    self.change(error=f"{type(error).__name__}: {error}")
            return event

    def health(
        self,
        stats: _JournalStats,
        *,
        queued_deliveries: int,
        inflight_deliveries: int,
    ) -> DeliveryHealth:
        with self.condition:
            counters = self._counters
            return DeliveryHealth(
                generation=self._generation,
                closed=self._closed or self._closing,
                outbox_items=stats.outbox_items,
                pending_inbox_items=stats.pending_inbox_items,
                acked_inbox_items=stats.acked_inbox_items,
                logical_storage_bytes=stats.logical_bytes,
                queued_deliveries=queued_deliveries,
                inflight_deliveries=inflight_deliveries,
                accepted=counters.accepted,
                frames_sent=counters.frames_sent,
                retries=counters.retries,
                delivered=counters.delivered,
                acknowledgements=counters.acknowledgements,
                negative_acknowledgements=counters.negative_acknowledgements,
                duplicates_suppressed=counters.duplicates_suppressed,
                expired_outbox=counters.expired_outbox,
                expired_inbox=counters.expired_inbox,
                last_error=self._last_error,
                append_outbox_items=stats.append_outbox_items,
                latest_outbox_items=stats.latest_outbox_items,
                terminal_inbox_items=stats.terminal_inbox_items,
                expired_inbox_items=stats.expired_inbox_items,
                peer_acknowledgements=counters.peer_acknowledgements,
                peer_negative_acknowledgements=(
                    counters.peer_negative_acknowledgements
                ),
                outbox_deduplicated=counters.outbox_deduplicated,
                coalesced=counters.coalesced,
                watermark_crossings=counters.watermark_crossings,
                expiry_sweeps=counters.expiry_sweeps,
                sweep_deleted_rows=counters.sweep_deleted_rows,
                recovered_watermarks=counters.recovered_watermarks,
                storage_rejections=counters.storage_rejections,
                terminal_drops=counters.terminal_drops,
                retry_exhausted=counters.retry_exhausted,
                ack_retry_exhausted=counters.ack_retry_exhausted,
                recovered_outbox=counters.recovered_outbox,
                transport_backpressure_failures=(
                    counters.transport_backpressure_failures
                ),
                transport_backpressure_streak=(
                    counters.transport_backpressure_streak
                ),
            )
