"""Interface adapters for external device and bus sources."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass

from .pubsub import PubSub, StreamRow


@dataclass(frozen=True)
class InterfaceEvent:
    """Normalized lifecycle, schema, data, or error event from an interface."""

    interface: str
    source_id: str
    event_type: str
    connected: bool
    sequence: int
    payload: str
    payload_encoding: str
    schema: str


class Interface:
    """Base adapter that publishes external-source events into PubSub.

    Interfaces model the messy edge of a system: devices connect, disappear,
    announce schemas, emit payloads, and fail. The event stream keeps those facts
    ordered and queryable without binding Manyfold to a specific Bluetooth,
    serial, or hardware library.
    """

    def __init__(
        self,
        name: str,
        *,
        topic: str | None = None,
        retained_messages: int = 1024,
        pubsub: PubSub | None = None,
    ) -> None:
        _require_text(name, "name")
        self.name = name
        self._pubsub = pubsub or PubSub(
            topic=topic or f"interface.{name}",
            schema=InterfaceEvent,
            retained_messages=retained_messages,
        )
        self._sequence = 0
        self._connected_sources: set[str] = set()

    @property
    def pubsub(self) -> PubSub:
        """Return the PubSub stream that carries this interface's events."""
        return self._pubsub

    @property
    def topic(self) -> str:
        """Return the PubSub topic for this interface."""
        return self._pubsub.topic

    def is_connected(self, source_id: str) -> bool:
        """Return whether the source is currently marked connected."""
        _require_text(source_id, "source_id")
        return source_id in self._connected_sources

    def connected_sources(self) -> tuple[str, ...]:
        """Return connected source identifiers in stable order."""
        return tuple(sorted(self._connected_sources))

    def connect(
        self,
        source_id: str,
        *,
        payload: object = "",
        schema: object = "",
        event_type: str = "connected",
    ) -> InterfaceEvent:
        """Mark a source connected and publish a connection event."""
        _require_text(source_id, "source_id")
        self._connected_sources.add(source_id)
        return self.emit(
            source_id,
            event_type=event_type,
            connected=True,
            payload=payload,
            schema=schema,
        )

    def disconnect(
        self,
        source_id: str,
        *,
        payload: object = "",
        event_type: str = "disconnected",
    ) -> InterfaceEvent:
        """Mark a source disconnected and publish a disconnection event."""
        _require_text(source_id, "source_id")
        self._connected_sources.discard(source_id)
        return self.emit(
            source_id,
            event_type=event_type,
            connected=False,
            payload=payload,
        )

    def publish_data(
        self,
        source_id: str,
        payload: object,
        *,
        schema: object = "",
        event_type: str = "data",
    ) -> InterfaceEvent:
        """Publish a data event from a source."""
        _require_text(source_id, "source_id")
        return self.emit(
            source_id,
            event_type=event_type,
            connected=self.is_connected(source_id),
            payload=payload,
            schema=schema,
        )

    def publish_schema(
        self,
        source_id: str,
        schema: object,
        *,
        payload: object = "",
        event_type: str = "schema",
    ) -> InterfaceEvent:
        """Publish a schema announcement for a source."""
        _require_text(source_id, "source_id")
        return self.emit(
            source_id,
            event_type=event_type,
            connected=self.is_connected(source_id),
            payload=payload,
            schema=schema,
        )

    def publish_error(
        self,
        source_id: str,
        error: BaseException | str,
        *,
        event_type: str = "error",
    ) -> InterfaceEvent:
        """Publish a source error without changing connection state."""
        _require_text(source_id, "source_id")
        if isinstance(error, BaseException):
            payload = f"{type(error).__name__}: {error}"
        else:
            payload = error
        return self.emit(
            source_id,
            event_type=event_type,
            connected=self.is_connected(source_id),
            payload=payload,
        )

    def emit(
        self,
        source_id: str,
        *,
        event_type: str,
        connected: bool,
        payload: object = "",
        schema: object = "",
    ) -> InterfaceEvent:
        """Publish one normalized event and return the emitted value."""
        _require_text(source_id, "source_id")
        _require_text(event_type, "event_type")
        encoded_payload = _encode_payload(payload)
        self._sequence += 1
        event = InterfaceEvent(
            interface=self.name,
            source_id=source_id,
            event_type=event_type,
            connected=connected,
            sequence=self._sequence,
            payload=encoded_payload.text,
            payload_encoding=encoded_payload.encoding,
            schema=_schema_text(schema),
        )
        self._pubsub.publish(event, key=source_id)
        return event

    def latest(self) -> StreamRow | None:
        """Return the latest interface event row."""
        return self._pubsub.latest()

    def query(
        self,
        sql: str,
        parameters: Mapping[str, object] | None = None,
    ) -> list[StreamRow]:
        """Run SQL against this interface's PubSub event stream."""
        return self._pubsub.query(sql, parameters)


class BluetoothControllerInterface(Interface):
    """Interface adapter for lossy Bluetooth controller lifecycles."""

    def connect_controller(
        self,
        controller_id: str,
        *,
        name: str = "",
        address: str = "",
    ) -> InterfaceEvent:
        """Publish that a controller is available for sampling."""
        return self.connect(
            controller_id,
            payload={"name": name, "address": address},
            event_type="controller.connected",
        )

    def disconnect_controller(
        self,
        controller_id: str,
        *,
        reason: str = "",
    ) -> InterfaceEvent:
        """Publish that a controller disappeared."""
        return self.disconnect(
            controller_id,
            payload=reason,
            event_type="controller.disconnected",
        )

    def publish_controller_state(
        self,
        controller_id: str,
        state: object,
        *,
        schema: object = "",
    ) -> InterfaceEvent:
        """Publish one sampled controller state payload."""
        return self.publish_data(
            controller_id,
            state,
            schema=schema,
            event_type="controller.state",
        )


class SerialBusInterface(Interface):
    """Interface adapter for serial buses that announce schemas and frames."""

    def __init__(
        self,
        name: str,
        *,
        topic: str | None = None,
        retained_messages: int = 1024,
        pubsub: PubSub | None = None,
    ) -> None:
        super().__init__(
            name,
            topic=topic,
            retained_messages=retained_messages,
            pubsub=pubsub,
        )
        self._schemas_by_source: dict[str, str] = {}

    def discover_bus(
        self,
        bus_id: str,
        *,
        schema: object = "",
        payload: object = "",
    ) -> InterfaceEvent:
        """Publish that a serial bus is currently present."""
        if schema != "":
            self._schemas_by_source[bus_id] = _schema_text(schema)
        return self.connect(
            bus_id,
            payload=payload,
            schema=schema,
            event_type="serial.connected",
        )

    def lose_bus(
        self,
        bus_id: str,
        *,
        reason: str = "",
    ) -> InterfaceEvent:
        """Publish that a serial bus is no longer present."""
        self._schemas_by_source.pop(bus_id, None)
        return self.disconnect(bus_id, payload=reason, event_type="serial.disconnected")

    def publish_bus_schema(
        self,
        bus_id: str,
        schema: object,
    ) -> InterfaceEvent:
        """Publish or replace the schema used to decode bus frames."""
        schema_text = _schema_text(schema)
        self._schemas_by_source[bus_id] = schema_text
        return self.publish_schema(
            bus_id,
            schema_text,
            event_type="serial.schema",
        )

    def publish_frame(
        self,
        bus_id: str,
        frame: object,
        *,
        schema: object = "",
    ) -> InterfaceEvent:
        """Publish one raw or decoded serial frame."""
        frame_schema = _schema_text(schema) if schema != "" else self.schema_for(bus_id)
        return self.publish_data(
            bus_id,
            frame,
            schema=frame_schema,
            event_type="serial.frame",
        )

    def schema_for(self, bus_id: str) -> str:
        """Return the latest known schema for a bus, or an empty string."""
        _require_text(bus_id, "bus_id")
        return self._schemas_by_source.get(bus_id, "")


def _encode_payload(payload: object) -> _EncodedPayload:
    if payload is None or payload == "":
        return _EncodedPayload("", "empty")
    if isinstance(payload, str):
        return _EncodedPayload(payload, "text")
    if isinstance(payload, bytes | bytearray | memoryview):
        return _EncodedPayload(bytes(payload).hex(), "hex")
    return _EncodedPayload(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str),
        "json",
    )


def _schema_text(schema: object) -> str:
    if schema is None or schema == "":
        return ""
    if isinstance(schema, str):
        return schema
    return json.dumps(schema, sort_keys=True, separators=(",", ":"), default=str)


def _require_text(value: str, field: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")


@dataclass(frozen=True)
class _EncodedPayload:
    text: str
    encoding: str


__all__ = [
    "BluetoothControllerInterface",
    "Interface",
    "InterfaceEvent",
    "SerialBusInterface",
]
