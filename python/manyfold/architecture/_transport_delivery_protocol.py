"""Private wire codec for durable transport delivery."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum

from .transport import FrameKind, TransportMessage

DELIVERY_PROTOCOL_VERSION = 1
DELIVERY_CHANNEL = "__manyfold.delivery.v1"

_DELIVERY_MAGIC = b"MFDL"
_DELIVERY_HEADER = struct.Struct("!4sBBBIHHHI")
_DELIVERY_HEADER_SIZE = _DELIVERY_HEADER.size
_MAX_MESSAGE_ID_BYTES = 128
_MAX_TEXT_BYTES = 65535


class DeliveryError(RuntimeError):
    """Base error for durable delivery failures."""


class DeliveryProtocolError(DeliveryError):
    """Raised for malformed durable-delivery control frames."""


def _encode_delivery_frame(
    operation: "_DeliveryOperation",
    message_id: str,
    *,
    frame_kind: int = 0,
    channel: str = "",
    correlation_id: str | None = None,
    payload: bytes = b"",
    delivery_attempt: int = 0,
) -> bytes:
    message_id_bytes = message_id.encode("utf-8")
    channel_bytes = channel.encode("utf-8")
    correlation_bytes = (
        b"" if correlation_id is None else correlation_id.encode("utf-8")
    )
    if len(message_id_bytes) > _MAX_MESSAGE_ID_BYTES:
        raise ValueError("encoded message_id is too long")
    if len(channel_bytes) > _MAX_TEXT_BYTES:
        raise ValueError("encoded delivery channel is too long")
    if len(correlation_bytes) > _MAX_TEXT_BYTES:
        raise ValueError("encoded delivery correlation_id is too long")
    header = _DELIVERY_HEADER.pack(
        _DELIVERY_MAGIC,
        DELIVERY_PROTOCOL_VERSION,
        int(operation),
        frame_kind,
        delivery_attempt,
        len(message_id_bytes),
        len(channel_bytes),
        len(correlation_bytes),
        len(payload),
    )
    return b"".join(
        (header, message_id_bytes, channel_bytes, correlation_bytes, payload)
    )


def _decode_delivery_frame(
    message: TransportMessage,
    *,
    max_message_bytes: int,
) -> "_DeliveryFrame":
    if message.kind is not FrameKind.PUBSUB or message.channel != DELIVERY_CHANNEL:
        raise DeliveryProtocolError(
            "durable delivery exclusively owns the transport receive stream"
        )
    if len(message.payload) < _DELIVERY_HEADER.size:
        raise DeliveryProtocolError("delivery frame is shorter than its header")
    (
        magic,
        version,
        operation,
        frame_kind,
        delivery_attempt,
        message_id_size,
        channel_size,
        correlation_size,
        payload_size,
    ) = _DELIVERY_HEADER.unpack(message.payload[: _DELIVERY_HEADER.size])
    if magic != _DELIVERY_MAGIC:
        raise DeliveryProtocolError("delivery frame magic is invalid")
    if version != DELIVERY_PROTOCOL_VERSION:
        raise DeliveryProtocolError(
            f"delivery protocol version {version} is incompatible"
        )
    if payload_size > max_message_bytes:
        raise DeliveryProtocolError(
            f"delivery payload exceeds max_message_bytes ({payload_size})"
        )
    if message_id_size > _MAX_MESSAGE_ID_BYTES:
        raise DeliveryProtocolError(
            "encoded delivery message_id exceeds the protocol limit"
        )
    expected_size = (
        _DELIVERY_HEADER.size
        + message_id_size
        + channel_size
        + correlation_size
        + payload_size
    )
    if len(message.payload) != expected_size:
        raise DeliveryProtocolError("delivery frame length does not match its header")
    offset = _DELIVERY_HEADER.size
    message_id_end = offset + message_id_size
    channel_end = message_id_end + channel_size
    correlation_end = channel_end + correlation_size
    try:
        message_id = message.payload[offset:message_id_end].decode("utf-8")
        channel = message.payload[message_id_end:channel_end].decode("utf-8")
        correlation_id = (
            None
            if correlation_size == 0
            else message.payload[channel_end:correlation_end].decode("utf-8")
        )
        resolved_operation = _DeliveryOperation(operation)
    except (UnicodeDecodeError, ValueError) as error:
        raise DeliveryProtocolError(
            f"delivery frame metadata is invalid: {error}"
        ) from error
    _require_text(message_id, "delivery message_id")
    if resolved_operation is _DeliveryOperation.DATA:
        try:
            FrameKind(frame_kind)
        except ValueError as error:
            raise DeliveryProtocolError(
                f"delivery frame kind {frame_kind} is invalid"
            ) from error
        _require_text(channel, "delivery channel")
        if delivery_attempt < 1:
            raise DeliveryProtocolError(
                "delivery DATA frame attempt must be positive"
            )
    elif (
        frame_kind != 0
        or delivery_attempt != 0
        or channel
        or correlation_id is not None
    ):
        raise DeliveryProtocolError("delivery control frame contains message metadata")
    if resolved_operation in (
        _DeliveryOperation.ACK,
        _DeliveryOperation.CONFIRM,
    ) and payload_size:
        raise DeliveryProtocolError(
            f"delivery {resolved_operation.name} frame contains a payload"
        )
    if (
        resolved_operation is _DeliveryOperation.NACK
        and payload_size > _MAX_TEXT_BYTES
    ):
        raise DeliveryProtocolError("delivery NACK reason exceeds the protocol limit")
    return _DeliveryFrame(
        operation=resolved_operation,
        message_id=message_id,
        frame_kind=frame_kind,
        delivery_attempt=delivery_attempt,
        channel=channel,
        correlation_id=correlation_id,
        payload=message.payload[correlation_end:],
    )


def _require_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeliveryProtocolError(f"{field_name} must be a non-empty string")
    return value.strip()


class _DeliveryOperation(IntEnum):
    DATA = 1
    ACK = 2
    NACK = 3
    CONFIRM = 4


@dataclass(frozen=True, slots=True)
class _DeliveryFrame:
    operation: _DeliveryOperation
    message_id: str
    frame_kind: int
    delivery_attempt: int
    channel: str
    correlation_id: str | None
    payload: bytes
