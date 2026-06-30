"""Rust-backed PubSub primitives for application architecture."""

from __future__ import annotations

import struct
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, is_dataclass
from time import time_ns
from types import NoneType
from typing import get_type_hints
from uuid import UUID, uuid5

from manyfold._manyfold_rust import (
    FlatBufferField,
    FlatBufferTable,
    InMemoryPubSub as InMemoryPubSub,
    PubSubDelivery as PubSubDelivery,
    PubSubMessage as PubSubMessage,
    PubSubRuntime,
    PubSubSubscription as PubSubSubscription,
)

_EPHEMERAL_TOPIC_NAMESPACE = UUID("6b9b5ff0-76e8-5c7f-b3b3-854f53ed8a3e")


class PubSub:
    """Topic-scoped PubSub stream with SQL-backed state."""

    def __init__(
        self,
        *,
        topic: str | None = None,
        schema: type | None = None,
        retained_messages: int = 1024,
    ) -> None:
        resolved_topic = _resolve_topic(topic)
        self.topic = resolved_topic
        self.schema = schema
        self._stream_schema = _coerce_stream_schema(resolved_topic, schema)
        self._runtime = PubSubRuntime(
            name=resolved_topic,
            retained_messages=retained_messages,
        )
        if self._stream_schema is not None:
            self._runtime.register_flatbuffer_pad(
                resolved_topic,
                self._stream_schema.table,
            )

    def publish(
        self,
        payload: object | None = None,
        *,
        event_time: int | None = None,
        key: str | None = None,
        **fields: object,
    ) -> None:
        """Publish a model value, bytes payload, or schema field values."""
        if fields:
            if payload is not None:
                raise ValueError("publish accepts either a payload or fields, not both")
            if self._stream_schema is None:
                raise ValueError("field publishing requires a stream schema")
            payload_bytes = self._stream_schema.encode(fields)
        elif payload is None:
            raise TypeError("publish requires a payload or fields")
        elif self._stream_schema is None and _is_model_value(payload):
            self._infer_stream_schema(type(payload))
            payload_bytes = self._stream_schema.encode(_value_fields(payload))
        elif self._stream_schema is not None and _is_schema_value(payload, self.schema):
            payload_bytes = self._stream_schema.encode(_value_fields(payload))
        elif self._stream_schema is not None and _is_model_value(payload):
            raise ValueError(
                "stream schema is already fixed; create another PubSub stream "
                "for a different model shape"
            )
        else:
            payload_bytes = _payload_bytes(payload)

        self._runtime.publish(
            self.topic,
            payload_bytes,
            pad_name=self.topic,
            event_time=event_time,
            key=key,
        )

    def query(
        self,
        sql: str,
        parameters: Mapping[str, object] | None = None,
    ) -> list[StreamRow]:
        """Run SQL against this stream's scoped ``stream`` relation."""
        query_parameters = dict(parameters or {})
        if "__manyfold_stream_topic" in query_parameters:
            raise ValueError("query parameter '__manyfold_stream_topic' is reserved")
        query_parameters["__manyfold_stream_topic"] = self.topic
        rows = self._runtime.query(
            f"""
            WITH stream AS (
                SELECT *
                FROM stream_messages
                WHERE topic = :__manyfold_stream_topic
                  AND pad_name = :__manyfold_stream_topic
            )
            {sql}
            """,
            query_parameters,
        )
        return [StreamRow(row) for row in rows]

    def query_one(
        self,
        sql: str,
        parameters: Mapping[str, object] | None = None,
    ) -> StreamRow | None:
        """Run SQL against this stream with at most one result row."""
        rows = self.query(sql, parameters)
        if len(rows) > 1:
            raise ValueError("stream query returned more than one row")
        return None if not rows else rows[0]

    def latest(self) -> StreamRow | None:
        """Return the latest stream row using SQL ordering."""
        return self.query_one(
            """
            SELECT *, offset + 1 AS seq_source
            FROM stream
            ORDER BY event_time DESC, process_sequence DESC
            LIMIT 1
            """
        )

    def average(self, *, field: str) -> float | None:
        """Return the average of a numeric stream field."""
        _require_sql_identifier(field, "field")
        row = self.query_one(
            f"""
            SELECT AVG({field}) AS average
            FROM stream
            """
        )
        if row is None or row.average is None:
            return None
        return float(row.average)

    def _infer_stream_schema(self, model: type) -> None:
        self.schema = model
        self._stream_schema = _coerce_stream_schema(self.topic, model)
        if self._stream_schema is None:
            raise ValueError("could not infer stream schema")
        self._runtime.register_flatbuffer_pad(
            self.topic,
            self._stream_schema.table,
        )


class StreamRow(Mapping[str, object]):
    """Read-only row returned from PubSub SQL queries."""

    def __init__(self, values: Mapping[str, object]) -> None:
        self._values = dict(values)

    def __getitem__(self, name: str) -> object:
        return self._values[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __getattr__(self, name: str) -> object:
        try:
            return self._values[name]
        except KeyError as error:
            raise AttributeError(name) from error

    def __eq__(self, other: object) -> bool:
        if isinstance(other, StreamRow):
            return self._values == other._values
        if isinstance(other, Mapping):
            return self._values == dict(other)
        return False

    def __repr__(self) -> str:
        return f"StreamRow({self._values!r})"

    def as_dict(self) -> dict[str, object]:
        """Return a plain dictionary copy of this row."""
        return dict(self._values)

    def as_model(self, model: type) -> object:
        """Construct a model from matching row fields."""
        fields = _schema_field_types(model)
        return model(**{name: self._values[name] for name in fields})


def _coerce_stream_schema(topic: str, value: type | None) -> _StreamSchema | None:
    if value is None:
        return None
    if isinstance(value, type):
        return _StreamSchema(
            name=_topic_schema_name(topic, value.__name__),
            field_types=_schema_field_types(value),
        )
    raise TypeError("schema must be a model class or None")


def _schema_field_types(model: type) -> dict[str, str]:
    model_fields = getattr(model, "model_fields", None)
    if isinstance(model_fields, Mapping):
        return {
            name: _field_type(getattr(field, "annotation", object))
            for name, field in model_fields.items()
        }
    legacy_fields = getattr(model, "__fields__", None)
    if isinstance(legacy_fields, Mapping):
        return {
            name: _field_type(getattr(field, "type_", object))
            for name, field in legacy_fields.items()
        }
    hints = get_type_hints(model)
    if not hints:
        raise ValueError("schema model must declare typed fields")
    return {name: _field_type(annotation) for name, annotation in hints.items()}


def _topic_schema_name(topic: str, schema_name: str) -> str:
    return f"{topic}.{schema_name}"


def _resolve_topic(topic: str | None) -> str:
    if topic is None:
        created_ns = time_ns()
        topic_id = uuid5(_EPHEMERAL_TOPIC_NAMESPACE, str(created_ns))
        return f"manyfold.ephemeral.{topic_id}"
    _require_text(topic, "topic")
    return topic


def _require_sql_identifier(value: str, field: str) -> None:
    if not isinstance(value, str) or not value.isidentifier():
        raise ValueError(f"{field} must be a SQL identifier")


def _field_type(annotation: object) -> str:
    if annotation in {float}:
        return "float64"
    if annotation in {int}:
        return "int64"
    if annotation in {str}:
        return "string"
    if annotation in {bool}:
        return "bool"
    if getattr(annotation, "__origin__", None) in {NoneType}:
        raise ValueError("optional schema fields are not supported yet")
    raise ValueError(f"unsupported schema field type: {annotation!r}")


def _is_schema_value(value: object, schema: object) -> bool:
    if isinstance(schema, type):
        return isinstance(value, schema)
    return False


def _is_model_value(value: object) -> bool:
    value_type = type(value)
    return (
        is_dataclass(value)
        or isinstance(getattr(value_type, "model_fields", None), Mapping)
        or isinstance(getattr(value_type, "__fields__", None), Mapping)
        or bool(get_type_hints(value_type))
    )


def _value_fields(value: object) -> Mapping[str, object]:
    if is_dataclass(value):
        return {
            name: getattr(value, name)
            for name in getattr(value, "__dataclass_fields__")
        }
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump()
        if isinstance(dumped, Mapping):
            return dumped
    dict_method = getattr(value, "dict", None)
    if callable(dict_method):
        dumped = dict_method()
        if isinstance(dumped, Mapping):
            return dumped
    annotations = get_type_hints(type(value))
    return {name: getattr(value, name) for name in annotations}


def _encode_flatbuffer(table: FlatBufferTable, values: Mapping[str, object]) -> bytes:
    fields = sorted(table.fields, key=lambda field: field.index)
    max_index = max(field.index for field in fields)
    object_bytes = bytearray(b"\x00\x00\x00\x00")
    field_offsets = [0] * (max_index + 1)
    string_patches: list[tuple[int, bytes]] = []

    for field in fields:
        if field.name not in values or values[field.name] is None:
            continue
        alignment = _flatbuffer_alignment(field.field_type)
        object_bytes.extend(
            b"\x00" * (_align(len(object_bytes), alignment) - len(object_bytes))
        )
        field_offset = len(object_bytes)
        field_offsets[field.index] = field_offset
        _append_flatbuffer_value(
            object_bytes,
            string_patches,
            field_offset,
            field.field_type,
            values[field.name],
        )

    vtable_length = 4 + 2 * len(field_offsets)
    table_position = 4 + vtable_length
    object_bytes[0:4] = struct.pack("<i", table_position - 4)
    vtable = bytearray(struct.pack("<HH", vtable_length, len(object_bytes)))
    for offset in field_offsets:
        vtable.extend(struct.pack("<H", offset))

    buffer = bytearray(struct.pack("<I", table_position))
    buffer.extend(vtable)
    buffer.extend(object_bytes)
    for field_offset, encoded in string_patches:
        field_position = table_position + field_offset
        string_position = len(buffer)
        buffer[field_position : field_position + 4] = struct.pack(
            "<I",
            string_position - field_position,
        )
        buffer.extend(struct.pack("<I", len(encoded)))
        buffer.extend(encoded)
        buffer.extend(b"\x00")
    return bytes(buffer)


def _append_flatbuffer_value(
    object_bytes: bytearray,
    string_patches: list[tuple[int, bytes]],
    field_offset: int,
    field_type: str,
    value: object,
) -> None:
    if field_type == "bool":
        object_bytes.extend(struct.pack("<?", bool(value)))
    elif field_type == "float32":
        object_bytes.extend(struct.pack("<f", float(value)))
    elif field_type == "float64":
        object_bytes.extend(struct.pack("<d", float(value)))
    elif field_type == "int32":
        object_bytes.extend(struct.pack("<i", int(value)))
    elif field_type == "int64":
        object_bytes.extend(struct.pack("<q", int(value)))
    elif field_type == "uint32":
        object_bytes.extend(struct.pack("<I", int(value)))
    elif field_type == "uint64":
        object_bytes.extend(struct.pack("<Q", int(value)))
    elif field_type == "string":
        object_bytes.extend(b"\x00\x00\x00\x00")
        string_patches.append((field_offset, str(value).encode("utf-8")))
    else:
        raise ValueError(f"unsupported FlatBuffer field type: {field_type!r}")


def _flatbuffer_alignment(field_type: str) -> int:
    if field_type in {"float64", "int64", "uint64"}:
        return 8
    if field_type in {"float32", "int32", "uint32", "string"}:
        return 4
    if field_type == "bool":
        return 1
    raise ValueError(f"unsupported FlatBuffer field type: {field_type!r}")


def _align(value: int, alignment: int) -> int:
    return value if value % alignment == 0 else value + alignment - value % alignment


def _payload_bytes(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray | memoryview):
        return bytes(value)
    output = getattr(value, "Output", None)
    if callable(output):
        return bytes(output())
    table = getattr(value, "_tab", None)
    if table is not None and hasattr(table, "Bytes"):
        return bytes(table.Bytes)
    try:
        return bytes(value)  # type: ignore[arg-type]
    except TypeError as error:
        raise TypeError(
            "payload must be bytes, generated FlatBuffer output, or a schema value"
        ) from error


def _require_text(value: str, field: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")


@dataclass(frozen=True)
class _StreamSchema:
    name: str
    field_types: Mapping[str, str]

    @property
    def table(self) -> FlatBufferTable:
        return FlatBufferTable(
            self.name,
            [
                FlatBufferField(field_name, index, field_type)
                for index, (field_name, field_type) in enumerate(self.field_types.items())
            ],
        )

    def encode(self, values: Mapping[str, object]) -> bytes:
        return _encode_flatbuffer(self.table, values)


__all__ = [
    "InMemoryPubSub",
    "PubSub",
    "PubSubDelivery",
    "PubSubMessage",
    "PubSubSubscription",
    "StreamRow",
]
