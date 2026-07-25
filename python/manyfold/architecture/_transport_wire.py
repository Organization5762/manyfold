"""Private binary framing and socket lifecycle for architecture transport."""

from __future__ import annotations

import socket
import struct
from dataclasses import dataclass
from threading import Event
from time import monotonic

PROTOCOL_NAME = "manyfold.transport"
PROTOCOL_VERSION = (1, 0)
MAGIC = b"MFCP"
HEADER = struct.Struct("!4sBBBBHHIQ")
HELLO_KIND = 0
HEARTBEAT_KIND = 255
MAX_IDENTITY_BYTES = 4096
MAX_TEXT_BYTES = 65535


def configure_socket(connection: socket.socket, peer_timeout: float) -> None:
    connection.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    connection.settimeout(peer_timeout)


def close_socket(connection: socket.socket | None) -> None:
    if connection is None:
        return
    try:
        connection.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    connection.close()


def encode_heartbeat(sequence: int) -> bytes:
    return encode_frame(HEARTBEAT_KIND, b"", b"", b"", sequence)


def encode_frame(
    kind: int,
    channel: bytes,
    correlation_id: bytes,
    payload: bytes,
    sequence: int,
) -> bytes:
    if len(channel) > MAX_TEXT_BYTES or len(correlation_id) > MAX_TEXT_BYTES:
        raise ValueError("encoded frame metadata is too long")
    header = HEADER.pack(
        MAGIC,
        PROTOCOL_VERSION[0],
        PROTOCOL_VERSION[1],
        kind,
        0,
        len(channel),
        len(correlation_id),
        len(payload),
        sequence,
    )
    return b"".join((header, channel, correlation_id, payload))


def read_frame(
    connection: socket.socket,
    *,
    max_payload_bytes: int,
    deadline: float,
    stop: Event,
) -> _WireFrame:
    header = _receive_exact(connection, HEADER.size, deadline=deadline, stop=stop)
    (
        magic,
        major,
        minor,
        kind,
        flags,
        channel_size,
        correlation_size,
        payload_size,
        sequence,
    ) = HEADER.unpack(header)
    if magic != MAGIC:
        raise _WireProtocolError("peer sent an invalid frame magic")
    if (major, minor) != PROTOCOL_VERSION:
        raise _WireProtocolError(
            "peer protocol version is incompatible "
            f"({major}.{minor} != {PROTOCOL_VERSION[0]}.{PROTOCOL_VERSION[1]})"
        )
    if flags != 0:
        raise _WireProtocolError(f"peer sent unsupported frame flags {flags}")
    if payload_size > max_payload_bytes:
        raise _WireProtocolError(
            "peer payload exceeds configured max_payload_bytes "
            f"({payload_size} > {max_payload_bytes})"
        )
    body_size = channel_size + correlation_size + payload_size
    body = _receive_exact(connection, body_size, deadline=deadline, stop=stop)
    channel_end = channel_size
    correlation_end = channel_end + correlation_size
    return _WireFrame(
        kind=kind,
        channel=body[:channel_end],
        correlation_id=body[channel_end:correlation_end],
        payload=body[correlation_end:],
        sequence=sequence,
        wire_size=HEADER.size + body_size,
    )


def _receive_exact(
    connection: socket.socket,
    size: int,
    *,
    deadline: float,
    stop: Event,
) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        if stop.is_set():
            raise _WireClosed("transport closed while receiving a frame")
        if monotonic() >= deadline:
            raise TimeoutError("peer did not complete a frame before timeout")
        try:
            chunk = connection.recv(remaining)
        except socket.timeout:
            continue
        if not chunk:
            raise ConnectionError("peer closed the connection")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


class _WireError(RuntimeError):
    pass


class _WireClosed(_WireError):
    pass


class _WireProtocolError(_WireError):
    pass


@dataclass(frozen=True, slots=True)
class _WireFrame:
    kind: int
    channel: bytes
    correlation_id: bytes
    payload: bytes
    sequence: int
    wire_size: int
