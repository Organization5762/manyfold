"""Committed delivery lifecycle observations shared by runtime components."""

from __future__ import annotations

from ._transport_delivery_events import (
    DeliveryCapacity,
    DeliveryEventKind,
    DeliveryOutcome,
    DeliveryStore,
)
from ._transport_delivery_records import _CompactionResult, _WatermarkCrossing
from ._transport_delivery_runtime import _DeliveryRuntime


def _emit_committed_compaction(
    runtime: _DeliveryRuntime,
    result: _CompactionResult,
    *,
    capacity: DeliveryCapacity | None = None,
    emit_empty_sweep: bool = False,
) -> None:
    for record in result.expired_outbox:
        runtime.emit(
            DeliveryEventKind.EXPIRED,
            record.message_id,
            record.channel,
            record.source_key,
            store=DeliveryStore.OUTBOX,
            correlation_id=record.correlation_id,
            attempt=record.attempts,
            outcome=DeliveryOutcome.expired("sender outbox expired"),
        )
    for record in result.expired_inbox:
        runtime.emit(
            DeliveryEventKind.EXPIRED,
            record.message_id,
            record.channel,
            None,
            store=DeliveryStore.INBOX,
            correlation_id=record.correlation_id,
            attempt=record.attempts,
            outcome=DeliveryOutcome.expired("receiver inbox expired"),
        )
    for record in result.retry_exhausted:
        runtime.emit(
            DeliveryEventKind.DROPPED,
            record.message_id,
            record.channel,
            record.source_key,
            store=DeliveryStore.OUTBOX,
            correlation_id=record.correlation_id,
            attempt=record.attempts,
            outcome=DeliveryOutcome.terminal("delivery attempts exhausted"),
        )
    if result.affected_items or emit_empty_sweep:
        runtime.emit(
            DeliveryEventKind.EXPIRY_SWEEP,
            None,
            None,
            None,
            capacity=capacity,
            affected_items=result.affected_items,
            deleted_items=result.deleted_items,
            released_logical_bytes=result.released_logical_bytes,
        )
        runtime.change(
            expiry_sweeps=1,
            sweep_deleted_rows=result.deleted_items,
            expired_outbox=len(result.expired_outbox),
            expired_inbox=len(result.expired_inbox),
            retry_exhausted=len(result.retry_exhausted),
        )


def _emit_committed_watermark(
    runtime: _DeliveryRuntime,
    *,
    topic: str,
    crossing: _WatermarkCrossing,
    store: DeliveryStore,
) -> None:
    for dimension in crossing.dimensions:
        runtime.emit(
            DeliveryEventKind.WATERMARK_CROSSED,
            None,
            topic,
            None,
            store=store,
            capacity_dimension=dimension,
            capacity=crossing.capacity,
        )
    runtime.change(watermark_crossings=len(crossing.dimensions))
