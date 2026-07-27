"""Private errors raised by the durable SQLite owner."""

from __future__ import annotations

import sqlite3
from typing import final

from ._transport_delivery_events import DeliveryCapacity


def _execute_write(
    connection: sqlite3.Connection,
    statement: str,
    parameters: tuple[object, ...],
) -> sqlite3.Cursor:
    try:
        return connection.execute(statement, parameters)
    except sqlite3.DatabaseError as error:
        raise _translate_sqlite_error(error, operation="write") from error


def _translate_sqlite_error(
    error: sqlite3.DatabaseError,
    *,
    operation: str,
) -> _JournalError:
    if (
        getattr(error, "sqlite_errorcode", None) == sqlite3.SQLITE_FULL
        or "database or disk is full" in str(error).lower()
    ):
        return _JournalFull(
            "delivery journal reached its SQLite page limit during "
            f"{operation}"
        )
    return _JournalError(
        f"could not {operation} delivery journal: {error}"
    )


class _JournalError(RuntimeError):
    pass


@final
class _JournalFull(_JournalError):
    def __init__(
        self,
        message: str,
        *,
        capacity: DeliveryCapacity | None = None,
    ) -> None:
        super().__init__(message)
        self.capacity = capacity


@final
class _JournalConflict(_JournalError):
    pass
