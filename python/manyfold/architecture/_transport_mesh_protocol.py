"""Private control and publication framing for the transport mesh."""

from __future__ import annotations

import json
import struct
from dataclasses import dataclass
from urllib.parse import quote, unquote

CONTROL_SUBSCRIBE = "_manyfold.mesh.subscribe"
CONTROL_UNSUBSCRIBE = "_manyfold.mesh.unsubscribe"
CONTROL_SYNC = "_manyfold.mesh.sync"
PUBLICATION_PREFIX = "_manyfold.mesh.publish/"
RESERVED_PREFIX = "_manyfold.mesh."
_PUBLICATION_HEADER = struct.Struct("!II")


def require_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def require_peer_node_id(value: str, local_node_id: str) -> str:
    peer_node_id = require_text(value, "peer node_id")
    if peer_node_id == local_node_id:
        raise ValueError("peer node_id must differ from local node_id")
    return peer_node_id


def require_topic(value: str) -> str:
    topic = require_text(value, "topic")
    if topic.startswith(RESERVED_PREFIX):
        raise ValueError(f"topic prefix {RESERVED_PREFIX!r} is reserved")
    return topic


def require_payload(value: bytes | bytearray | memoryview) -> bytes:
    if not isinstance(value, bytes | bytearray | memoryview):
        raise TypeError("payload must be bytes-like")
    return bytes(value)


def require_positive_integer(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")


def require_timeout(value: float | None) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int | float) or value < 0:
        raise ValueError("timeout must be a non-negative number or None")


def encode_subscription(subscription_id: str, topic: str) -> bytes:
    return json.dumps(
        {"subscription_id": subscription_id, "topic": topic},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def decode_subscription(payload: bytes) -> tuple[str, str]:
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("subscription control payload is not valid JSON") from error
    if not isinstance(value, dict):
        raise ValueError("subscription control payload must be a JSON object")
    try:
        subscription_id = value["subscription_id"]
        topic = value["topic"]
    except KeyError as error:
        raise ValueError(
            f"subscription control payload is missing {error.args[0]!r}"
        ) from error
    if not isinstance(subscription_id, str) or not subscription_id.strip():
        raise ValueError("subscription_id must be a non-empty string")
    return subscription_id.strip(), require_topic(topic)


def encode_publication_channel(source_node_id: str, topic: str) -> str:
    return (
        f"{PUBLICATION_PREFIX}{quote(source_node_id, safe='')}/"
        f"{quote(topic, safe='')}"
    )


def decode_publication_channel(channel: str) -> tuple[str, str]:
    encoded = channel.removeprefix(PUBLICATION_PREFIX)
    try:
        encoded_source, encoded_topic = encoded.split("/", 1)
        source_node_id = unquote(encoded_source, errors="strict")
        topic = unquote(encoded_topic, errors="strict")
    except ValueError as error:
        raise ValueError("publication channel is missing source or topic") from error
    except UnicodeDecodeError as error:
        raise ValueError("publication source or topic is not valid UTF-8") from error
    if not source_node_id.strip():
        raise ValueError("publication source_node_id must be a non-empty string")
    return source_node_id.strip(), require_topic(topic)


def encode_publication_payload(
    payload: bytes,
    correlation_id: str | None,
    replacement_key: str | None = None,
) -> bytes:
    """Frame an optional application correlation separately from message identity."""
    correlation = (
        b"" if correlation_id is None else correlation_id.encode("utf-8")
    )
    replacement = (
        b"" if replacement_key is None else replacement_key.encode("utf-8")
    )
    return (
        _PUBLICATION_HEADER.pack(len(correlation), len(replacement))
        + correlation
        + replacement
        + payload
    )


def decode_publication_payload(
    payload: bytes,
) -> tuple[str | None, str | None, bytes]:
    """Decode one framed application correlation and payload."""
    if len(payload) < _PUBLICATION_HEADER.size:
        raise ValueError("publication payload is truncated")
    correlation_size, replacement_size = _PUBLICATION_HEADER.unpack_from(payload)
    correlation_end = _PUBLICATION_HEADER.size + correlation_size
    replacement_end = correlation_end + replacement_size
    if replacement_end > len(payload):
        raise ValueError("publication metadata is truncated")
    try:
        correlation_id = payload[_PUBLICATION_HEADER.size : correlation_end].decode(
            "utf-8"
        )
        replacement_key = payload[correlation_end:replacement_end].decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("publication metadata is not valid UTF-8") from error
    return (
        correlation_id or None,
        replacement_key or None,
        payload[replacement_end:],
    )


def encode_durable_correlation(
    source_node_id: str,
    replacement_key: str | None,
    correlation_id: str,
) -> str:
    """Encode private durable routing metadata into one transport correlation."""
    return json.dumps(
        {
            "correlation_id": require_text(correlation_id, "correlation_id"),
            "replacement_key": replacement_key,
            "source_node_id": require_text(source_node_id, "source_node_id"),
        },
        separators=(",", ":"),
        sort_keys=True,
    )


def decode_durable_correlation(value: str | None) -> _DurableCorrelation:
    """Decode and validate private durable routing metadata."""
    if value is None:
        raise ValueError("durable topic correlation metadata is missing")
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as error:
        raise ValueError("durable topic correlation metadata is invalid") from error
    if not isinstance(decoded, dict):
        raise ValueError("durable topic correlation metadata must be an object")
    try:
        source_node_id = decoded["source_node_id"]
        replacement_key = decoded["replacement_key"]
        correlation_id = decoded["correlation_id"]
    except KeyError as error:
        raise ValueError(
            f"durable topic correlation metadata is missing {error.args[0]!r}"
        ) from error
    if replacement_key is not None and not isinstance(replacement_key, str):
        raise ValueError("durable replacement_key must be a string or null")
    return _DurableCorrelation(
        require_text(source_node_id, "durable source_node_id"),
        replacement_key,
        require_text(correlation_id, "durable correlation_id"),
    )


@dataclass(frozen=True, slots=True)
class _DurableCorrelation:
    source_node_id: str
    replacement_key: str | None
    correlation_id: str
