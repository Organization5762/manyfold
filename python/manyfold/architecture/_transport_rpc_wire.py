"""Private binary record framing for coordinator RPC."""

from __future__ import annotations

import struct
from dataclasses import dataclass

from .transport import FrameKind, TransportMessage

REQUEST_CHANNEL = "manyfold.rpc.request"
RESPONSE_CHANNEL = "manyfold.rpc.response"
ERROR_CHANNEL = "manyfold.rpc.error"
CANCEL_CHANNEL = "manyfold.rpc.cancel"

_SESSION_HEADER = struct.Struct("!HH")
_REQUEST_HEADER = struct.Struct("!dHH")
_ERROR_HEADER = struct.Struct("!?HI")


def encode_request(
    *,
    correlation_id: str,
    service: str,
    method: str,
    payload: bytes,
    timeout_seconds: float,
    session_id: str,
    target_session_id: str,
) -> TransportMessage:
    session_prefix = _encode_session_ids(session_id, target_session_id)
    service_bytes = service.encode("utf-8")
    method_bytes = method.encode("utf-8")
    if len(service_bytes) > 65535 or len(method_bytes) > 65535:
        raise ValueError("encoded RPC service or method is too long")
    framed_payload = b"".join(
        (
            session_prefix,
            _REQUEST_HEADER.pack(
                timeout_seconds,
                len(service_bytes),
                len(method_bytes),
            ),
            service_bytes,
            method_bytes,
            payload,
        )
    )
    return TransportMessage(
        FrameKind.RPC_REQUEST,
        REQUEST_CHANNEL,
        framed_payload,
        correlation_id=correlation_id,
    )


def encode_response(
    *,
    correlation_id: str,
    payload: bytes,
    session_id: str,
    target_session_id: str,
) -> TransportMessage:
    session_prefix = _encode_session_ids(session_id, target_session_id)
    return TransportMessage(
        FrameKind.RPC_RESPONSE,
        RESPONSE_CHANNEL,
        session_prefix + payload,
        correlation_id=correlation_id,
    )


def encode_error(
    *,
    correlation_id: str,
    code: str,
    message: str,
    retryable: bool,
    session_id: str,
    target_session_id: str,
) -> TransportMessage:
    session_prefix = _encode_session_ids(session_id, target_session_id)
    code_bytes = code.encode("utf-8")
    message_bytes = message.encode("utf-8")
    if len(code_bytes) > 65535:
        raise ValueError("encoded RPC error code is too long")
    payload = b"".join(
        (
            session_prefix,
            _ERROR_HEADER.pack(retryable, len(code_bytes), len(message_bytes)),
            code_bytes,
            message_bytes,
        )
    )
    return TransportMessage(
        FrameKind.RPC_ERROR,
        ERROR_CHANNEL,
        payload,
        correlation_id=correlation_id,
    )


def encode_cancel(
    *,
    correlation_id: str,
    reason: str,
    session_id: str,
    target_session_id: str,
) -> TransportMessage:
    session_prefix = _encode_session_ids(session_id, target_session_id)
    return TransportMessage(
        FrameKind.RPC_ERROR,
        CANCEL_CHANNEL,
        (
            session_prefix + reason.encode("utf-8")
        ),
        correlation_id=correlation_id,
    )


def decode(message: TransportMessage) -> "_DecodedRecord":
    correlation_id = message.correlation_id
    if correlation_id is None:
        raise _WireProtocolError("RPC frame is missing correlation_id")
    if (
        message.kind is FrameKind.RPC_REQUEST
        and message.channel == REQUEST_CHANNEL
    ):
        return _decode_request(correlation_id, message.payload)
    if (
        message.kind is FrameKind.RPC_RESPONSE
        and message.channel == RESPONSE_CHANNEL
    ):
        session_id, target_session_id, payload = _decode_session_ids(
            message.payload
        )
        return _DecodedResponse(
            correlation_id,
            payload,
            session_id,
            target_session_id,
        )
    if message.kind is FrameKind.RPC_ERROR and message.channel == ERROR_CHANNEL:
        return _decode_error(correlation_id, message.payload)
    if message.kind is FrameKind.RPC_ERROR and message.channel == CANCEL_CHANNEL:
        session_id, target_session_id, payload = _decode_session_ids(
            message.payload
        )
        try:
            return _DecodedCancel(
                correlation_id,
                payload.decode("utf-8"),
                session_id,
                target_session_id,
            )
        except UnicodeDecodeError as error:
            raise _WireProtocolError(
                "RPC cancellation reason is not UTF-8"
            ) from error
    raise _WireProtocolError(
        f"unexpected RPC frame kind/channel {message.kind.name}/{message.channel!r}"
    )


def _decode_request(correlation_id: str, payload: bytes) -> "_DecodedRequest":
    session_id, target_session_id, payload = _decode_session_ids(payload)
    if len(payload) < _REQUEST_HEADER.size:
        raise _WireProtocolError("RPC request header is truncated")
    timeout, service_size, method_size = _REQUEST_HEADER.unpack_from(payload)
    metadata_end = _REQUEST_HEADER.size + service_size + method_size
    if metadata_end > len(payload):
        raise _WireProtocolError("RPC request metadata lengths exceed payload")
    service_end = _REQUEST_HEADER.size + service_size
    try:
        service = payload[_REQUEST_HEADER.size:service_end].decode("utf-8")
        method = payload[service_end:metadata_end].decode("utf-8")
    except UnicodeDecodeError as error:
        raise _WireProtocolError("RPC request metadata is not UTF-8") from error
    return _DecodedRequest(
        correlation_id,
        service,
        method,
        payload[metadata_end:],
        timeout,
        session_id,
        target_session_id,
    )


def _decode_error(correlation_id: str, payload: bytes) -> "_DecodedError":
    session_id, target_session_id, payload = _decode_session_ids(payload)
    if len(payload) < _ERROR_HEADER.size:
        raise _WireProtocolError("RPC error header is truncated")
    retryable, code_size, message_size = _ERROR_HEADER.unpack_from(payload)
    body_end = _ERROR_HEADER.size + code_size + message_size
    if body_end != len(payload):
        raise _WireProtocolError("RPC error metadata lengths do not match payload")
    code_end = _ERROR_HEADER.size + code_size
    try:
        code = payload[_ERROR_HEADER.size:code_end].decode("utf-8")
        message = payload[code_end:body_end].decode("utf-8")
    except UnicodeDecodeError as error:
        raise _WireProtocolError("RPC error metadata is not UTF-8") from error
    return _DecodedError(
        correlation_id,
        code,
        message,
        retryable,
        session_id,
        target_session_id,
    )


def _encode_session_ids(session_id: str, target_session_id: str) -> bytes:
    session_bytes = session_id.encode("utf-8")
    target_bytes = target_session_id.encode("utf-8")
    if not session_bytes or len(session_bytes) > 65535:
        raise ValueError("encoded RPC session_id must be 1..65535 bytes")
    if len(target_bytes) > 65535:
        raise ValueError("encoded RPC target_session_id exceeds 65535 bytes")
    return (
        _SESSION_HEADER.pack(len(session_bytes), len(target_bytes))
        + session_bytes
        + target_bytes
    )


def _decode_session_ids(payload: bytes) -> tuple[str, str, bytes]:
    if len(payload) < _SESSION_HEADER.size:
        raise _WireProtocolError("RPC session header is truncated")
    session_size, target_size = _SESSION_HEADER.unpack_from(payload)
    session_end = _SESSION_HEADER.size + session_size
    target_end = session_end + target_size
    if session_size == 0 or target_end > len(payload):
        raise _WireProtocolError("RPC session length is invalid")
    try:
        session_id = payload[_SESSION_HEADER.size:session_end].decode("utf-8")
        target_session_id = payload[session_end:target_end].decode("utf-8")
    except UnicodeDecodeError as error:
        raise _WireProtocolError("RPC session metadata is not UTF-8") from error
    return session_id, target_session_id, payload[target_end:]


class _WireProtocolError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class _DecodedRequest:
    correlation_id: str
    service: str
    method: str
    payload: bytes
    timeout_seconds: float
    session_id: str
    target_session_id: str


@dataclass(frozen=True, slots=True)
class _DecodedResponse:
    correlation_id: str
    payload: bytes
    session_id: str
    target_session_id: str


@dataclass(frozen=True, slots=True)
class _DecodedError:
    correlation_id: str
    code: str
    message: str
    retryable: bool
    session_id: str
    target_session_id: str


@dataclass(frozen=True, slots=True)
class _DecodedCancel:
    correlation_id: str
    reason: str
    session_id: str
    target_session_id: str


_DecodedRecord = _DecodedRequest | _DecodedResponse | _DecodedError | _DecodedCancel
