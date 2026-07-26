"""Private control and publication framing for the transport mesh."""

from __future__ import annotations

import json

CONTROL_SUBSCRIBE = "_manyfold.mesh.subscribe"
CONTROL_UNSUBSCRIBE = "_manyfold.mesh.unsubscribe"
CONTROL_SYNC = "_manyfold.mesh.sync"
RESERVED_PREFIX = "_manyfold.mesh."


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
