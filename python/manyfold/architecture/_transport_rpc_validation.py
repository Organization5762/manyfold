"""Private validation helpers for coordinator RPC contracts."""

from __future__ import annotations

from math import isfinite


def format_session_id(instance_id: str, connection_number: int) -> str:
    return f"{instance_id}:{connection_number}"


def parse_session_id(session_id: str) -> tuple[str, int]:
    instance_id, separator, encoded_connection = session_id.rpartition(":")
    if not separator or not instance_id:
        raise ValueError("session_id must contain an instance and connection number")
    try:
        connection_number = int(encoded_connection)
    except ValueError as error:
        raise ValueError("session connection number must be an integer") from error
    if connection_number < 1:
        raise ValueError("session connection number must be positive")
    return instance_id, connection_number


def require_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def require_bytes(value: bytes, field_name: str) -> bytes:
    if not isinstance(value, bytes | bytearray | memoryview):
        raise TypeError(f"{field_name} must be bytes-like")
    return bytes(value)


def require_positive_integer(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")


def require_positive_number(value: float, field_name: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{field_name} must be a positive number")


def require_nonnegative_number(value: float, field_name: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not isfinite(value)
        or value < 0
    ):
        raise ValueError(f"{field_name} must be a non-negative number")
