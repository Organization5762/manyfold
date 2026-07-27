"""SQLite schema initialization and bounded migration for delivery journals."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from uuid import uuid4

from ._transport_delivery_capacity import _journal_stats
from ._transport_delivery_journal_errors import _JournalFull
from ._transport_delivery_migration import (
    _compact_migrated_v1,
    _preflight_v1,
)
from ._transport_delivery_policy import DeliveryConfig
from ._transport_delivery_records import (
    _inbox_logical_bytes,
    _InboxRecord,
    _outbox_logical_bytes,
    _OutboxRecord,
)
from ._transport_delivery_recovery import (
    _RecoveryCapacityViolation,
    _RecoveryViolation,
    _validate_recovery,
)

_JOURNAL_APPLICATION_ID = 0x4D46444C
_JOURNAL_SCHEMA_VERSION = 2
_MAX_MESSAGE_SEQUENCE = (1 << 64) - 1
_V2_TABLE_COLUMNS = {
    "outbox": (
        "message_id",
        "channel",
        "semantics",
        "source_key",
        "frame_kind",
        "correlation_id",
        "payload",
        "created_at",
        "expires_at",
        "attempts",
        "max_attempts",
        "next_attempt_at",
        "size_bytes",
    ),
    "inbox": (
        "message_id",
        "frame_kind",
        "channel",
        "correlation_id",
        "payload",
        "delivery_attempt",
        "status",
        "created_at",
        "expires_at",
        "ack_attempts",
        "next_ack_at",
        "ack_confirmed",
        "outcome_reason",
        "rejection_only",
        "size_bytes",
    ),
    "journal_metadata": ("key", "value"),
}
_V2_INDEX_COLUMNS = {
    "outbox_due": ("next_attempt_at", "created_at", "message_id"),
    "outbox_recovery": (
        "channel",
        "created_at",
        "message_id",
        "semantics",
        "source_key",
        "size_bytes",
        "attempts",
        "max_attempts",
        "expires_at",
    ),
    "outbox_replay": (
        "created_at",
        "message_id",
        "channel",
        "source_key",
        "correlation_id",
        "attempts",
    ),
    "outbox_latest_source": ("channel", "source_key"),
    "inbox_status": ("status", "created_at", "message_id"),
    "inbox_ack_due": (
        "status",
        "ack_confirmed",
        "next_ack_at",
        "message_id",
    ),
    "inbox_recovery": (
        "channel",
        "created_at",
        "message_id",
        "rejection_only",
        "size_bytes",
        "delivery_attempt",
        "ack_attempts",
        "expires_at",
        "status",
    ),
}
_SCHEMA = """
CREATE TABLE IF NOT EXISTS outbox (
    message_id TEXT PRIMARY KEY,
    channel TEXT NOT NULL,
    semantics TEXT NOT NULL CHECK(semantics IN ('append', 'latest')),
    source_key TEXT,
    frame_kind INTEGER NOT NULL,
    correlation_id TEXT,
    payload BLOB NOT NULL,
    created_at REAL NOT NULL,
    expires_at REAL NOT NULL,
    attempts INTEGER NOT NULL CHECK(attempts >= 0),
    max_attempts INTEGER NOT NULL CHECK(max_attempts >= 1),
    next_attempt_at REAL NOT NULL,
    size_bytes INTEGER NOT NULL CHECK(size_bytes >= 160),
    CHECK(
        (semantics = 'append' AND source_key IS NULL)
        OR (semantics = 'latest' AND source_key IS NOT NULL)
    ),
    CHECK(attempts <= max_attempts),
    CHECK(expires_at >= created_at),
    CHECK(length(trim(message_id)) > 0),
    CHECK(length(trim(channel)) > 0),
    CHECK(source_key IS NULL OR length(trim(source_key)) > 0),
    CHECK(correlation_id IS NULL OR length(trim(correlation_id)) > 0),
    CHECK(message_id = trim(message_id)),
    CHECK(channel = trim(channel)),
    CHECK(source_key IS NULL OR source_key = trim(source_key)),
    CHECK(correlation_id IS NULL OR correlation_id = trim(correlation_id)),
    CHECK(frame_kind IN (1, 2, 3, 4)),
    CHECK(frame_kind = 1 OR correlation_id IS NOT NULL),
    CHECK(typeof(payload) = 'blob'),
    CHECK(typeof(frame_kind) = 'integer'),
    CHECK(typeof(attempts) = 'integer'),
    CHECK(typeof(max_attempts) = 'integer'),
    CHECK(typeof(size_bytes) = 'integer'),
    CHECK(typeof(message_id) = 'text'),
    CHECK(typeof(channel) = 'text'),
    CHECK(typeof(semantics) = 'text'),
    CHECK(source_key IS NULL OR typeof(source_key) = 'text'),
    CHECK(correlation_id IS NULL OR typeof(correlation_id) = 'text')
);
CREATE INDEX IF NOT EXISTS outbox_due
ON outbox(next_attempt_at, created_at, message_id);
CREATE INDEX IF NOT EXISTS outbox_recovery
ON outbox(
    channel, created_at, message_id, semantics, source_key, size_bytes,
    attempts, max_attempts, expires_at
);
CREATE INDEX IF NOT EXISTS outbox_replay
ON outbox(
    created_at, message_id, channel, source_key, correlation_id, attempts
);
CREATE UNIQUE INDEX IF NOT EXISTS outbox_latest_source
ON outbox(channel, source_key) WHERE semantics = 'latest';
CREATE TABLE IF NOT EXISTS inbox (
    message_id TEXT PRIMARY KEY,
    frame_kind INTEGER NOT NULL,
    channel TEXT NOT NULL,
    correlation_id TEXT,
    payload BLOB NOT NULL,
    delivery_attempt INTEGER NOT NULL CHECK(delivery_attempt >= 1),
    status TEXT NOT NULL
        CHECK(status IN ('pending', 'acked', 'terminal', 'expired')),
    created_at REAL NOT NULL,
    expires_at REAL NOT NULL,
    ack_attempts INTEGER NOT NULL CHECK(ack_attempts >= 0),
    next_ack_at REAL NOT NULL,
    ack_confirmed INTEGER NOT NULL CHECK(ack_confirmed IN (0, 1)),
    outcome_reason TEXT,
    rejection_only INTEGER NOT NULL CHECK(rejection_only IN (0, 1)),
    size_bytes INTEGER NOT NULL CHECK(size_bytes >= 128),
    CHECK(expires_at >= created_at),
    CHECK(length(trim(message_id)) > 0),
    CHECK(length(trim(channel)) > 0),
    CHECK(correlation_id IS NULL OR length(trim(correlation_id)) > 0),
    CHECK(message_id = trim(message_id)),
    CHECK(channel = trim(channel)),
    CHECK(correlation_id IS NULL OR correlation_id = trim(correlation_id)),
    CHECK(frame_kind IN (1, 2, 3, 4)),
    CHECK(frame_kind = 1 OR correlation_id IS NOT NULL),
    CHECK(typeof(payload) = 'blob'),
    CHECK(typeof(frame_kind) = 'integer'),
    CHECK(typeof(delivery_attempt) = 'integer'),
    CHECK(typeof(ack_attempts) = 'integer'),
    CHECK(typeof(ack_confirmed) = 'integer'),
    CHECK(typeof(rejection_only) = 'integer'),
    CHECK(typeof(size_bytes) = 'integer'),
    CHECK(
        (
            status = 'pending'
            AND outcome_reason IS NULL
            AND ack_attempts = 0
            AND ack_confirmed = 0
            AND rejection_only = 0
        )
        OR (
            status = 'acked'
            AND outcome_reason IS NULL
            AND rejection_only = 0
        )
        OR (
            status IN ('terminal', 'expired')
            AND outcome_reason IS NOT NULL
            AND length(trim(outcome_reason)) > 0
            AND outcome_reason = trim(outcome_reason)
        )
    ),
    CHECK(
        rejection_only = 0
        OR (
            status = 'terminal'
            AND length(payload) = 0
        )
    ),
    CHECK(typeof(message_id) = 'text'),
    CHECK(typeof(channel) = 'text'),
    CHECK(correlation_id IS NULL OR typeof(correlation_id) = 'text'),
    CHECK(outcome_reason IS NULL OR typeof(outcome_reason) = 'text')
);
CREATE INDEX IF NOT EXISTS inbox_status
ON inbox(status, created_at, message_id);
CREATE INDEX IF NOT EXISTS inbox_ack_due
ON inbox(status, ack_confirmed, next_ack_at, message_id);
CREATE INDEX IF NOT EXISTS inbox_recovery
ON inbox(
    channel, created_at, message_id, rejection_only, size_bytes,
    delivery_attempt, ack_attempts, expires_at, status
);
CREATE TABLE IF NOT EXISTS journal_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    CHECK(typeof(key) = 'text'),
    CHECK(typeof(value) = 'text')
);
"""


def _initialize_schema(
    connection: sqlite3.Connection,
    *,
    config: DeliveryConfig,
    recovery_now: float,
) -> None:
    application_id = int(connection.execute("PRAGMA application_id").fetchone()[0])
    schema_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    if application_id not in (0, _JOURNAL_APPLICATION_ID):
        raise sqlite3.DatabaseError("file is not a ManyFold delivery journal")
    if schema_version not in (0, 1, _JOURNAL_SCHEMA_VERSION):
        raise sqlite3.DatabaseError(
            "delivery journal schema version "
            f"{schema_version} is incompatible with {_JOURNAL_SCHEMA_VERSION}"
        )
    encoding = str(connection.execute("PRAGMA encoding").fetchone()[0])
    if encoding.casefold().replace("-", "") != "utf8":
        raise sqlite3.DatabaseError(
            "delivery journal must use SQLite UTF-8 encoding"
        )
    _require_integrity(connection)
    if schema_version == _JOURNAL_SCHEMA_VERSION:
        if application_id != _JOURNAL_APPLICATION_ID:
            raise sqlite3.DatabaseError(
                "delivery journal V2 is missing its application identity"
            )
        _require_v2_schema(connection)
        return
    if schema_version == 0:
        _require_empty_schema(connection)
        _initialize_v2(connection)
    else:
        if application_id != _JOURNAL_APPLICATION_ID:
            raise sqlite3.DatabaseError(
                "delivery journal V1 is missing its application identity"
            )
        outbox_rows, inbox_rows = _preflight_v1(
            connection,
            recovery_batch_size=config.recovery_batch_size,
        )
        _migrate_v1(
            connection,
            config=config,
            recovery_now=recovery_now,
            retained_rows=outbox_rows + inbox_rows,
        )
    _require_v2_schema(connection)
    _require_integrity(connection)


def _initialize_v2(connection: sqlite3.Connection) -> None:
    connection.execute("BEGIN IMMEDIATE")
    try:
        _execute_script_in_transaction(connection, _SCHEMA)
        connection.execute(f"PRAGMA application_id={_JOURNAL_APPLICATION_ID}")
        connection.execute(f"PRAGMA user_version={_JOURNAL_SCHEMA_VERSION}")
        _initialize_metadata(connection)
        connection.execute("COMMIT")
    except BaseException:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise


def _migrate_v1(
    connection: sqlite3.Connection,
    *,
    config: DeliveryConfig,
    recovery_now: float,
    retained_rows: int,
) -> None:
    attempts_by_channel = {
        policy.topic: policy.max_attempts for policy in config.topic_policies
    }
    connection.execute("BEGIN IMMEDIATE")
    try:
        _create_migration_tables(connection)
        _copy_v1_outbox(
            connection,
            recovery_batch_size=config.recovery_batch_size,
            legacy_attempts_by_channel=attempts_by_channel,
            legacy_default_max_attempts=config.max_delivery_attempts,
        )
        _copy_v1_inbox(
            connection,
            recovery_batch_size=config.recovery_batch_size,
        )
        _execute_script_in_transaction(
            connection,
            """
            DROP TABLE outbox;
            DROP TABLE inbox;
            ALTER TABLE outbox_v2 RENAME TO outbox;
            ALTER TABLE inbox_v2 RENAME TO inbox;
            """,
        )
        _execute_script_in_transaction(connection, _SCHEMA)
        _compact_migrated_v1(
            connection,
            recovery_now=recovery_now,
            recovery_batch_size=config.recovery_batch_size,
            retained_rows=retained_rows,
            dedupe_retention_seconds=config.dedupe_retention_seconds,
        )
        try:
            _validate_recovery(
                connection,
                config,
                {
                    policy.topic: policy
                    for policy in config.topic_policies
                },
                _journal_stats(connection),
                max_transport_payload_bytes=(1 << 63) - 1,
                recovery_now=recovery_now,
                enforce_bounds=True,
            )
        except _RecoveryCapacityViolation as error:
            raise _JournalFull(
                "legacy delivery journal exceeds current bounds: "
                f"{error}",
                capacity=error.capacity,
            ) from error
        except _RecoveryViolation as error:
            raise sqlite3.DatabaseError(
                f"migrated delivery journal is invalid: {error}"
            ) from error
        connection.execute(f"PRAGMA application_id={_JOURNAL_APPLICATION_ID}")
        connection.execute(f"PRAGMA user_version={_JOURNAL_SCHEMA_VERSION}")
        _initialize_metadata(connection)
        connection.execute("COMMIT")
    except BaseException:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise


def _copy_v1_outbox(
    connection: sqlite3.Connection,
    *,
    recovery_batch_size: int,
    legacy_attempts_by_channel: Mapping[str, int],
    legacy_default_max_attempts: int,
) -> None:
    after_rowid = 0
    while True:
        rows = connection.execute(
            """
            SELECT rowid, message_id, channel, frame_kind, correlation_id,
                   payload, attempts
            FROM outbox
            WHERE rowid > ?
            ORDER BY rowid
            LIMIT ?
            """,
            (after_rowid, recovery_batch_size),
        ).fetchall()
        if not rows:
            return
        inserts: list[tuple[object, ...]] = []
        for row in rows:
            (
                rowid,
                message_id,
                channel,
                frame_kind,
                correlation_id,
                payload,
                attempts,
            ) = row
            channel_name = str(channel)
            record = _OutboxRecord(
                str(message_id),
                channel_name,
                "append",
                None,
                int(frame_kind),
                None if correlation_id is None else str(correlation_id),
                bytes(payload),
                int(attempts),
                max(
                    int(attempts),
                    legacy_attempts_by_channel.get(
                        channel_name,
                        legacy_default_max_attempts,
                    ),
                ),
            )
            source = connection.execute(
                """
                SELECT created_at, expires_at, next_attempt_at
                FROM outbox WHERE rowid = ?
                """,
                (rowid,),
            ).fetchone()
            inserts.append(
                (
                    record.message_id,
                    record.channel,
                    record.semantics,
                    record.source_key,
                    record.frame_kind,
                    record.correlation_id,
                    record.payload,
                    float(source[0]),
                    float(source[1]),
                    record.attempts,
                    record.max_attempts,
                    float(source[2]),
                    _outbox_logical_bytes(record),
                )
            )
        connection.executemany(
            """
            INSERT INTO outbox_v2 (
                message_id, channel, semantics, source_key, frame_kind,
                correlation_id, payload, created_at, expires_at, attempts,
                max_attempts, next_attempt_at, size_bytes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            inserts,
        )
        after_rowid = int(rows[-1][0])


def _copy_v1_inbox(
    connection: sqlite3.Connection,
    *,
    recovery_batch_size: int,
) -> None:
    after_rowid = 0
    while True:
        rows = connection.execute(
            """
            SELECT rowid, message_id, frame_kind, channel, correlation_id,
                   payload, delivery_attempt, ack_attempts
            FROM inbox
            WHERE rowid > ?
            ORDER BY rowid
            LIMIT ?
            """,
            (after_rowid, recovery_batch_size),
        ).fetchall()
        if not rows:
            return
        inserts: list[tuple[object, ...]] = []
        for row in rows:
            (
                rowid,
                message_id,
                frame_kind,
                channel,
                correlation_id,
                payload,
                delivery_attempt,
                ack_attempts,
            ) = row
            record = _InboxRecord(
                str(message_id),
                int(frame_kind),
                str(channel),
                None if correlation_id is None else str(correlation_id),
                bytes(payload),
                int(delivery_attempt),
                int(ack_attempts),
            )
            source = connection.execute(
                """
                SELECT status, created_at, expires_at, next_ack_at, ack_confirmed
                FROM inbox WHERE rowid = ?
                """,
                (rowid,),
            ).fetchone()
            record = _InboxRecord(
                record.message_id,
                record.frame_kind,
                record.channel,
                record.correlation_id,
                record.payload,
                record.delivery_attempt,
                record.ack_attempts,
                str(source[0]),
            )
            inserts.append(
                (
                    record.message_id,
                    record.frame_kind,
                    record.channel,
                    record.correlation_id,
                    record.payload,
                    record.delivery_attempt,
                    record.status,
                    float(source[1]),
                    float(source[2]),
                    record.ack_attempts,
                    float(source[3]),
                    int(source[4]),
                    None,
                    0,
                    _inbox_logical_bytes(record),
                )
            )
        connection.executemany(
            """
            INSERT INTO inbox_v2 (
                message_id, frame_kind, channel, correlation_id, payload,
                delivery_attempt, status, created_at, expires_at, ack_attempts,
                next_ack_at, ack_confirmed, outcome_reason, rejection_only,
                size_bytes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            inserts,
        )
        after_rowid = int(rows[-1][0])


def _initialize_metadata(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        INSERT OR IGNORE INTO journal_metadata (key, value)
        VALUES ('message_namespace', ?)
        """,
        (uuid4().hex,),
    )
    connection.execute(
        """
        INSERT OR IGNORE INTO journal_metadata (key, value)
        VALUES ('message_sequence', '0')
        """
    )


def _require_integrity(connection: sqlite3.Connection) -> None:
    result = tuple(
        str(row[0])
        for row in connection.execute("PRAGMA quick_check(1)").fetchall()
    )
    if result != ("ok",):
        raise sqlite3.DatabaseError(
            f"delivery journal integrity check failed: {result!r}"
        )


def _require_empty_schema(connection: sqlite3.Connection) -> None:
    objects = connection.execute(
        """
        SELECT name FROM sqlite_master
        WHERE type IN ('table', 'index')
          AND name NOT LIKE 'sqlite_%'
        """
    ).fetchall()
    if objects:
        raise sqlite3.DatabaseError(
            "unversioned delivery journal contains unexpected schema objects"
        )


def _require_v2_schema(connection: sqlite3.Connection) -> None:
    active_objects = connection.execute(
        """
        SELECT type, name FROM sqlite_master
        WHERE type IN ('view', 'trigger')
          AND name NOT LIKE 'sqlite_%'
        """
    ).fetchall()
    if active_objects:
        raise sqlite3.DatabaseError(
            "delivery journal V2 contains unexpected views or triggers"
        )
    tables = {
        str(row[0])
        for row in connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
            """
        )
    }
    if tables != set(_V2_TABLE_COLUMNS):
        raise sqlite3.DatabaseError(
            f"delivery journal V2 table contract is invalid: {sorted(tables)!r}"
        )
    for table, expected_columns in _V2_TABLE_COLUMNS.items():
        table_sql = str(
            connection.execute(
                """
                SELECT sql FROM sqlite_master
                WHERE type = 'table' AND name = ?
                """,
                (table,),
            ).fetchone()[0]
        )
        expected_sql = next(
            statement
            for statement in _SCHEMA.split(";")
            if statement.strip().startswith(
                f"CREATE TABLE IF NOT EXISTS {table} "
            )
        )
        normalized_expected = _normalize_schema_sql(expected_sql)
        normalized_actual = _normalize_schema_sql(table_sql)
        if normalized_actual not in {
            normalized_expected,
            normalized_expected.replace("ifnotexists", "", 1),
        }:
            raise sqlite3.DatabaseError(
                f"delivery journal V2 {table} definition is invalid"
            )
        columns = tuple(
            str(row[1])
            for row in connection.execute(
                f"PRAGMA table_xinfo({table})"
            ).fetchall()
            if int(row[6]) == 0
        )
        if columns != expected_columns:
            raise sqlite3.DatabaseError(
                f"delivery journal V2 {table} columns are invalid"
            )
    implicit_indexes = {
        (str(row[0]), str(row[1]))
        for row in connection.execute(
            """
            SELECT name, tbl_name FROM sqlite_master
            WHERE type = 'index' AND sql IS NULL
            """
        )
    }
    if implicit_indexes != {
        ("sqlite_autoindex_outbox_1", "outbox"),
        ("sqlite_autoindex_inbox_1", "inbox"),
        ("sqlite_autoindex_journal_metadata_1", "journal_metadata"),
    }:
        raise sqlite3.DatabaseError(
            "delivery journal V2 primary-key index contract is invalid"
        )
    indexes = {
        str(row[0]): str(row[1])
        for row in connection.execute(
            """
            SELECT name, tbl_name FROM sqlite_master
            WHERE type = 'index' AND sql IS NOT NULL
            """
        )
    }
    if set(indexes) != set(_V2_INDEX_COLUMNS):
        raise sqlite3.DatabaseError(
            "delivery journal V2 index contract is invalid"
        )
    for index, expected_columns in _V2_INDEX_COLUMNS.items():
        expected_table = "outbox" if index.startswith("outbox_") else "inbox"
        if indexes[index] != expected_table:
            raise sqlite3.DatabaseError(
                f"delivery journal V2 index {index!r} owner is invalid"
            )
        columns = tuple(
            str(row[2])
            for row in connection.execute(
                f"PRAGMA index_info({index})"
            ).fetchall()
        )
        if columns != expected_columns:
            raise sqlite3.DatabaseError(
                f"delivery journal V2 index {index!r} is invalid"
            )
        properties = next(
            row
            for row in connection.execute(
                f"PRAGMA index_list({expected_table})"
            ).fetchall()
            if str(row[1]) == index
        )
        expected_unique = index == "outbox_latest_source"
        if bool(properties[2]) is not expected_unique or bool(
            properties[4]
        ) is not expected_unique:
            raise sqlite3.DatabaseError(
                f"delivery journal V2 index {index!r} properties are invalid"
            )
    latest_sql = connection.execute(
        """
        SELECT sql FROM sqlite_master
        WHERE type = 'index' AND name = 'outbox_latest_source'
        """
    ).fetchone()
    if not _normalize_schema_sql(str(latest_sql[0])).endswith(
        "wheresemantics='latest'"
    ):
        raise sqlite3.DatabaseError(
            "delivery journal V2 latest-source index predicate is invalid"
        )
    _require_v2_constraints(connection)
    metadata = dict(
        connection.execute(
            """
            SELECT key, value FROM journal_metadata
            WHERE key IN ('message_namespace', 'message_sequence')
            """
        ).fetchall()
    )
    namespace = str(metadata.get("message_namespace", ""))
    if (
        len(namespace) != 32
        or namespace != namespace.lower()
        or any(character not in "0123456789abcdef" for character in namespace)
    ):
        raise sqlite3.DatabaseError(
            "delivery journal V2 message namespace is invalid"
        )
    sequence_text = metadata.get("message_sequence")
    try:
        sequence = int(sequence_text)
    except (KeyError, TypeError, ValueError) as error:
        raise sqlite3.DatabaseError(
            "delivery journal V2 message sequence is invalid"
        ) from error
    if (
        str(sequence) != sequence_text
        or not 0 <= sequence <= _MAX_MESSAGE_SEQUENCE
    ):
        raise sqlite3.DatabaseError(
            "delivery journal V2 message sequence is invalid"
        )


def _require_v2_constraints(connection: sqlite3.Connection) -> None:
    expected = {
        "outbox": (
            "check(semanticsin('append','latest'))",
            "check(attempts>=0)",
            "check(max_attempts>=1)",
            "check(attempts<=max_attempts)",
            "check(expires_at>=created_at)",
            "check(size_bytes>=160)",
            "semantics='append'andsource_keyisnull",
            "semantics='latest'andsource_keyisnotnull",
            "check(length(trim(message_id))>0)",
            "check(length(trim(channel))>0)",
            "check(frame_kindin(1,2,3,4))",
            "check(frame_kind=1orcorrelation_idisnotnull)",
            "check(typeof(payload)='blob')",
        ),
        "inbox": (
            "check(delivery_attempt>=1)",
            "check(statusin('pending','acked','terminal','expired'))",
            "check(ack_attempts>=0)",
            "check(ack_confirmedin(0,1))",
            "check(rejection_onlyin(0,1))",
            "check(expires_at>=created_at)",
            "check(size_bytes>=128)",
            "check(length(trim(message_id))>0)",
            "check(length(trim(channel))>0)",
            "check(frame_kindin(1,2,3,4))",
            "check(frame_kind=1orcorrelation_idisnotnull)",
            "check(typeof(payload)='blob')",
            "status='pending'andoutcome_reasonisnullandack_attempts=0",
            "status='acked'andoutcome_reasonisnullandrejection_only=0",
            "statusin('terminal','expired')andoutcome_reasonisnotnull",
            "rejection_only=0or(status='terminal'andlength(payload)=0)",
        ),
    }
    for table, fragments in expected.items():
        row = connection.execute(
            """
            SELECT sql FROM sqlite_master
            WHERE type = 'table' AND name = ?
            """,
            (table,),
        ).fetchone()
        normalized = _normalize_schema_sql(str(row[0]))
        if any(fragment not in normalized for fragment in fragments):
            raise sqlite3.DatabaseError(
                f"delivery journal V2 {table} constraints are invalid"
            )


def _normalize_schema_sql(value: str) -> str:
    return "".join(value.lower().replace('"', "").split())


def _execute_script_in_transaction(
    connection: sqlite3.Connection,
    script: str,
) -> None:
    for statement in script.split(";"):
        stripped = statement.strip()
        if stripped:
            connection.execute(stripped)


def _create_migration_tables(connection: sqlite3.Connection) -> None:
    for statement in _SCHEMA.split(";"):
        stripped = statement.strip()
        if stripped.startswith("CREATE TABLE IF NOT EXISTS outbox "):
            connection.execute(
                stripped.replace(
                    "CREATE TABLE IF NOT EXISTS outbox ",
                    "CREATE TABLE outbox_v2 ",
                    1,
                )
            )
        elif stripped.startswith("CREATE TABLE IF NOT EXISTS inbox "):
            connection.execute(
                stripped.replace(
                    "CREATE TABLE IF NOT EXISTS inbox ",
                    "CREATE TABLE inbox_v2 ",
                    1,
                )
            )
