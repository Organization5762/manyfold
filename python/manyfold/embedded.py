"""Embedded-device profile helpers for the RFC 21 device shape."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, TypeVar

from ._manyfold_rust import Layer, Plane, Variant
from .primitives import OwnerName, Schema, StreamFamily, StreamName, TypedRoute, route

T = TypeVar("T")
TMeta = TypeVar("TMeta")

_BULK_CREDIT_POLICY = "bytes"

# Validation issue order is part of the ergonomics: callers can show the tuple
# directly and get stable, high-priority setup fixes before softer advice.
_FIRMWARE_ISSUE_CHECKS: tuple[tuple[str, str], ...] = (
    ("route_descriptors", "firmware agent must provide route descriptors"),
    ("sequence_numbering", "firmware agent must provide sequence numbering"),
    ("source_timestamping", "firmware agent must timestamp close to the source"),
    ("transport_framing", "firmware agent must provide transport framing"),
    ("shadow_reporting", "firmware agent should expose shadow reporting"),
    ("local_filtering", "firmware agent should support local filtering"),
    ("local_aggregation", "firmware agent should support local aggregation"),
    ("ring_buffer_staging", "firmware agent should stage through a ring buffer"),
)
_EMBEDDED_RUNTIME_ISSUE_CHECKS: tuple[tuple[str, str], ...] = (
    (
        "timestamps_close_to_source",
        "embedded routes must timestamp close to the source",
    ),
    ("keep_isr_work_minimal", "embedded routes must keep ISR work minimal"),
    (
        "use_dma_or_async_peripherals",
        "embedded routes should prefer DMA or async peripherals",
    ),
    ("bounded_ring_buffers", "embedded routes must use bounded ring buffers"),
    (
        "avoid_heap_on_hot_paths",
        "embedded routes should avoid heap allocation on hot paths",
    ),
    (
        "separate_metadata_and_payload_early",
        "embedded routes should separate metadata and payload early",
    ),
    (
        "preserve_device_and_ingest_time",
        "embedded routes must preserve device time and ingest time separately",
    ),
)
_BULK_RUNTIME_ISSUE_CHECKS: tuple[tuple[str, str], ...] = (
    ("lazy_bulk_payload_open", "bulk payload opening should be lazy"),
    (
        "prefer_zero_copy_bulk_payloads",
        "bulk payload paths should prefer zero-copy or shared memory strategies",
    ),
)
__all__ = (
    "EmbeddedBulkSensor",
    "EmbeddedDeviceProfile",
    "EmbeddedRuntimeRules",
    "EmbeddedScalarSensor",
    "FirmwareAgentProfile",
)


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

    def __post_init__(self) -> None:
        _require_bool_fields(
            self,
            tuple(field for field, _message in _FIRMWARE_ISSUE_CHECKS)
            + ("flash_backed_retention",),
        )

    def required_issues(self) -> tuple[str, ...]:
        return _disabled_issue_messages(self, _FIRMWARE_ISSUE_CHECKS)


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
    bulk_credit_policy: str = _BULK_CREDIT_POLICY

    def __post_init__(self) -> None:
        _require_bool_fields(
            self,
            tuple(field for field, _message in _EMBEDDED_RUNTIME_ISSUE_CHECKS)
            + tuple(field for field, _message in _BULK_RUNTIME_ISSUE_CHECKS),
        )
        _require_non_blank_string(self.bulk_credit_policy, "bulk_credit_policy")

    def required_issues(self) -> tuple[str, ...]:
        return _disabled_issue_messages(self, _EMBEDDED_RUNTIME_ISSUE_CHECKS)

    def bulk_issues(self) -> tuple[str, ...]:
        issues = self.required_issues() + _disabled_issue_messages(
            self, _BULK_RUNTIME_ISSUE_CHECKS
        )
        if self.bulk_credit_policy != _BULK_CREDIT_POLICY:
            issues += (
                "bulk payload routes must use byte credits instead of count credits",
            )
        return issues


@dataclass(frozen=True)
class EmbeddedScalarSensor(Generic[T]):
    metadata_route: TypedRoute[T]
    firmware: FirmwareAgentProfile = field(default_factory=FirmwareAgentProfile)
    rules: EmbeddedRuntimeRules = field(default_factory=EmbeddedRuntimeRules)

    def validate(self) -> tuple[str, ...]:
        issues = list(self.firmware.required_issues())
        issues.extend(self.rules.required_issues())
        if self.metadata_route.plane != Plane.Read:
            issues.append("embedded sensor metadata must flow in the read plane")
        if self.metadata_route.layer == Layer.Bulk:
            issues.append("embedded sensor metadata must not use Layer.Bulk")
        if self.metadata_route.variant != Variant.Meta:
            issues.append("embedded sensor metadata must use Variant.Meta")
        return tuple(issues)


@dataclass(frozen=True)
class EmbeddedBulkSensor(Generic[TMeta]):
    metadata_route: TypedRoute[TMeta]
    payload_route: TypedRoute[bytes]
    firmware: FirmwareAgentProfile = field(default_factory=FirmwareAgentProfile)
    rules: EmbeddedRuntimeRules = field(default_factory=EmbeddedRuntimeRules)

    def validate(self) -> tuple[str, ...]:
        issues = list(self.firmware.required_issues())
        issues.extend(self.rules.bulk_issues())
        if self.metadata_route.plane != Plane.Read:
            issues.append("bulk sensor metadata must flow in the read plane")
        if self.metadata_route.layer == Layer.Bulk:
            issues.append("bulk sensor metadata must not use Layer.Bulk")
        if self.metadata_route.variant != Variant.Meta:
            issues.append("bulk sensor metadata must use Variant.Meta")
        if self.payload_route.plane != Plane.Read:
            issues.append("bulk sensor payload must flow in the read plane")
        if self.payload_route.layer != Layer.Bulk:
            issues.append("bulk sensor payload must use Layer.Bulk")
        if self.payload_route.variant != Variant.Payload:
            issues.append("bulk sensor payload must use Variant.Payload")
        if self.metadata_route.owner != self.payload_route.owner:
            issues.append("bulk sensor metadata and payload owners must match")
        if self.metadata_route.family != self.payload_route.family:
            issues.append("bulk sensor metadata and payload families must match")
        return tuple(issues)


@dataclass(frozen=True)
class EmbeddedDeviceProfile:
    """Factory for the embedded route shapes used by the examples."""

    firmware: FirmwareAgentProfile = field(default_factory=FirmwareAgentProfile)
    rules: EmbeddedRuntimeRules = field(default_factory=EmbeddedRuntimeRules)

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


def _disabled_issue_messages(
    owner: object, checks: tuple[tuple[str, str], ...]
) -> tuple[str, ...]:
    return tuple(message for field, message in checks if not getattr(owner, field))


def _require_bool(value: object, field: str) -> None:
    if not isinstance(value, bool):
        raise ValueError(f"{field} must be a boolean")


def _require_bool_fields(owner: object, fields: tuple[str, ...]) -> None:
    for field_name in fields:
        _require_bool(getattr(owner, field_name), field_name)


def _require_non_blank_string(value: object, field: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
