"""Adapt typed coordinator RPC records to private binary wire frames."""

from __future__ import annotations

from . import _transport_rpc_wire as _wire
from ._transport_rpc_types import (
    RpcCancel,
    RpcErrorRecord,
    RpcProtocolError,
    RpcRequest,
    RpcResponse,
)
from .transport import TransportMessage


def encode(
    record: RpcRequest | RpcResponse | RpcErrorRecord | RpcCancel,
) -> TransportMessage:
    if isinstance(record, RpcRequest):
        return _wire.encode_request(
            correlation_id=record.correlation_id,
            service=record.service,
            method=record.method,
            payload=record.payload,
            timeout_seconds=record.timeout_seconds,
            session_id=record.session_id,
            target_session_id=record.target_session_id,
        )
    if isinstance(record, RpcResponse):
        return _wire.encode_response(
            correlation_id=record.correlation_id,
            payload=record.payload,
            session_id=record.session_id,
            target_session_id=record.target_session_id,
        )
    if isinstance(record, RpcErrorRecord):
        return _wire.encode_error(
            correlation_id=record.correlation_id,
            code=record.code,
            message=record.message,
            retryable=record.retryable,
            session_id=record.session_id,
            target_session_id=record.target_session_id,
        )
    return _wire.encode_cancel(
        correlation_id=record.correlation_id,
        reason=record.reason,
        session_id=record.session_id,
        target_session_id=record.target_session_id,
    )


def decode(
    message: TransportMessage,
) -> RpcRequest | RpcResponse | RpcErrorRecord | RpcCancel:
    try:
        decoded = _wire.decode(message)
        if isinstance(decoded, _wire._DecodedRequest):
            return RpcRequest(
                decoded.correlation_id,
                decoded.service,
                decoded.method,
                decoded.payload,
                decoded.timeout_seconds,
                decoded.session_id,
                decoded.target_session_id,
            )
        if isinstance(decoded, _wire._DecodedResponse):
            return RpcResponse(
                decoded.correlation_id,
                decoded.payload,
                decoded.session_id,
                decoded.target_session_id,
            )
        if isinstance(decoded, _wire._DecodedError):
            return RpcErrorRecord(
                decoded.correlation_id,
                decoded.code,
                decoded.message,
                decoded.retryable,
                decoded.session_id,
                decoded.target_session_id,
            )
        return RpcCancel(
            decoded.correlation_id,
            decoded.reason,
            decoded.session_id,
            decoded.target_session_id,
        )
    except (_wire._WireProtocolError, TypeError, ValueError) as error:
        raise RpcProtocolError(f"RPC wire record is invalid: {error}") from error
