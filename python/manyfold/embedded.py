"""Embedded-device profile helpers for the RFC 21 device shape."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

from ._manyfold_rust import Layer, Plane, Variant
from .primitives import OwnerName, Schema, StreamFamily, StreamName, TypedRoute, route

T = TypeVar("T")
TMeta = TypeVar("TMeta")


@dataclass(frozen=True)
class FirmwareAgentProfile:
    """Capabilities expected from the firmware-lite agent profile."""

    route_descriptors: bool = True
    sequence_numbering: bool = True
    source_timestamping: bool = True
    transport_framing: bool = True
    shadow_reporting: bool = True
    local_filtering: bool = True
    local_aggregation: bool = True
    ring_buffer_staging: bool = True
    flash_backed_retention: bool = False

    def required_issues(self) -> tuple[str, ...]:
        issues: list[str] = []
        if not self.route_descriptors:
            issues.append("firmware agent must provide route descriptors")
        if not self.sequence_numbering:
            issues.append("firmware agent must provide sequence numbering")
        if not self.source_timestamping:
            issues.append("firmware agent must timestamp close to the source")
        if not self.transport_framing:
            issues.append("firmware agent must provide transport framing")
        if not self.shadow_reporting:
            issues.append("firmware agent should expose shadow reporting")
        if not self.local_filtering:
            issues.append("firmware agent should support local filtering")
        if not self.local_aggregation:
            issues.append("firmware agent should support local aggregation")
        if not self.ring_buffer_staging:
            issues.append("firmware agent should stage through a ring buffer")
        return tuple(issues)


@dataclass(frozen=True)
class EmbeddedRuntimeRules:
    """Concrete embedded rules derived from RFC 21.2 and 21.3."""

    timestamps_close_to_source: bool = True
    keep_isr_work_minimal: bool = True
    use_dma_or_async_peripherals: bool = True
    bounded_ring_buffers: bool = True
    avoid_heap_on_hot_paths: bool = True
    separate_metadata_and_payload_early: bool = True
    preserve_device_and_ingest_time: bool = True
    lazy_bulk_payload_open: bool = True
    prefer_zero_copy_bulk_payloads: bool = True
    bulk_credit_policy: str = "bytes"

    def required_issues(self) -> tuple[str, ...]:
        issues: list[str] = []
        if not self.timestamps_close_to_source:
            issues.append("embedded routes must timestamp close to the source")
        if not self.keep_isr_work_minimal:
            issues.append("embedded routes must keep ISR work minimal")
        if not self.use_dma_or_async_peripherals:
            issues.append("embedded routes should prefer DMA or async peripherals")
        if not self.bounded_ring_buffers:
            issues.append("embedded routes must use bounded ring buffers")
        if not self.avoid_heap_on_hot_paths:
            issues.append("embedded routes should avoid heap allocation on hot paths")
        if not self.separate_metadata_and_payload_early:
            issues.append("embedded routes should separate metadata and payload early")
        if not self.preserve_device_and_ingest_time:
            issues.append(
                "embedded routes must preserve device time and ingest time separately"
            )
        return tuple(issues)

    def bulk_issues(self) -> tuple[str, ...]:
        issues = list(self.required_issues())
        if not self.lazy_bulk_payload_open:
            issues.append("bulk payload opening should be lazy")
        if not self.prefer_zero_copy_bulk_payloads:
            issues.append(
                "bulk payload paths should prefer zero-copy or shared memory strategies"
            )
        if self.bulk_credit_policy != "bytes":
            issues.append(
                "bulk payload routes must use byte credits instead of count credits"
            )
        return tuple(issues)


@dataclass(frozen=True)
class EmbeddedScalarSensor(Generic[T]):
    metadata_route: TypedRoute[T]
    firmware: FirmwareAgentProfile = FirmwareAgentProfile()
    rules: EmbeddedRuntimeRules = EmbeddedRuntimeRules()

    def validate(self) -> tuple[str, ...]:
        issues = list(self.firmware.required_issues())
        issues.extend(self.rules.required_issues())
        if self.metadata_route.plane != Plane.Read:
            issues.append("embedded sensor metadata must flow in the read plane")
        if self.metadata_route.variant != Variant.Meta:
            issues.append("embedded sensor metadata must use Variant.Meta")
        return tuple(issues)


@dataclass(frozen=True)
class EmbeddedBulkSensor(Generic[TMeta]):
    metadata_route: TypedRoute[TMeta]
    payload_route: TypedRoute[bytes]
    firmware: FirmwareAgentProfile = FirmwareAgentProfile()
    rules: EmbeddedRuntimeRules = EmbeddedRuntimeRules()

    def validate(self) -> tuple[str, ...]:
        issues = list(self.firmware.required_issues())
        issues.extend(self.rules.bulk_issues())
        if self.metadata_route.plane != Plane.Read:
            issues.append("bulk sensor metadata must flow in the read plane")
        if self.metadata_route.variant != Variant.Meta:
            issues.append("bulk sensor metadata must use Variant.Meta")
        if self.payload_route.plane != Plane.Read:
            issues.append("bulk sensor payload must flow in the read plane")
        if self.payload_route.layer != Layer.Bulk:
            issues.append("bulk sensor payload must use Layer.Bulk")
        if self.payload_route.variant != Variant.Payload:
            issues.append("bulk sensor payload must use Variant.Payload")
        return tuple(issues)


@dataclass(frozen=True)
class EmbeddedDeviceProfile:
    """Factory for the embedded route shapes used by the examples."""

    firmware: FirmwareAgentProfile = FirmwareAgentProfile()
    rules: EmbeddedRuntimeRules = EmbeddedRuntimeRules()

    def scalar_sensor(
        self,
        *,
        owner: OwnerName,
        family: StreamFamily,
        stream: StreamName,
        schema: Schema[T],
        layer: Layer = Layer.Raw,
    ) -> EmbeddedScalarSensor[T]:
        return EmbeddedScalarSensor(
            metadata_route=route(
                plane=Plane.Read,
                layer=layer,
                owner=owner,
                family=family,
                stream=stream,
                variant=Variant.Meta,
                schema=schema,
            ),
            firmware=self.firmware,
            rules=self.rules,
        )

    def bulk_sensor(
        self,
        *,
        owner: OwnerName,
        family: StreamFamily,
        metadata_stream: StreamName,
        metadata_schema: Schema[TMeta],
        payload_stream: StreamName,
        payload_schema: Schema[bytes],
    ) -> EmbeddedBulkSensor[TMeta]:
        return EmbeddedBulkSensor(
            metadata_route=route(
                plane=Plane.Read,
                layer=Layer.Logical,
                owner=owner,
                family=family,
                stream=metadata_stream,
                variant=Variant.Meta,
                schema=metadata_schema,
            ),
            payload_route=route(
                plane=Plane.Read,
                layer=Layer.Bulk,
                owner=owner,
                family=family,
                stream=payload_stream,
                variant=Variant.Payload,
                schema=payload_schema,
            ),
            firmware=self.firmware,
            rules=self.rules,
        )
