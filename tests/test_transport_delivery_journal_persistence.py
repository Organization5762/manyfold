from __future__ import annotations

import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path

from manyfold.architecture._transport_delivery_journal import _DeliveryJournal
from manyfold.architecture._transport_delivery_journal_errors import (
    _JournalError,
    _JournalFull,
)
from manyfold.architecture._transport_delivery_outcomes import (
    _TerminalRejectionDisposition,
)
from manyfold.architecture._transport_delivery_policy import (
    DeliveryConfig,
    TopicDeliveryPolicy,
)
from manyfold.architecture._transport_delivery_records import (
    _inbox_logical_bytes,
    _InboxRecord,
    _outbox_logical_bytes,
    _OutboxRecord,
)
from manyfold.architecture._transport_delivery_schema import (
    _MAX_MESSAGE_SEQUENCE,
    _initialize_schema,
)

_APPLICATION_ID = 0x4D46444C
_V1_SCHEMA = """
CREATE TABLE outbox (
    message_id TEXT PRIMARY KEY,
    frame_kind INTEGER NOT NULL,
    channel TEXT NOT NULL,
    correlation_id TEXT,
    payload BLOB NOT NULL,
    created_at REAL NOT NULL,
    expires_at REAL NOT NULL,
    attempts INTEGER NOT NULL,
    next_attempt_at REAL NOT NULL,
    last_error TEXT,
    size_bytes INTEGER NOT NULL
);
CREATE INDEX outbox_due ON outbox(next_attempt_at, created_at);
CREATE TABLE inbox (
    message_id TEXT PRIMARY KEY,
    frame_kind INTEGER NOT NULL,
    channel TEXT NOT NULL,
    correlation_id TEXT,
    payload BLOB NOT NULL,
    delivery_attempt INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'acked')),
    created_at REAL NOT NULL,
    expires_at REAL NOT NULL,
    ack_attempts INTEGER NOT NULL,
    next_ack_at REAL NOT NULL,
    ack_confirmed INTEGER NOT NULL CHECK(ack_confirmed IN (0, 1)),
    size_bytes INTEGER NOT NULL
);
CREATE INDEX inbox_pending ON inbox(status, created_at);
CREATE INDEX inbox_ack_due ON inbox(status, ack_confirmed, next_ack_at);
"""


class TransportDeliveryJournalPersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory()
        self._root = Path(self._temporary_directory.name)

    def tearDown(self) -> None:
        self._temporary_directory.cleanup()

    def test_v2_logical_bytes_count_each_authoritative_field_once(self) -> None:
        append = _OutboxRecord(
            "mé",
            "orders.debug_state",
            "append",
            None,
            1,
            "κ",
            b"abc",
            0,
            8,
        )
        latest = _OutboxRecord(
            "latest",
            "state",
            "latest",
            "source-α",
            1,
            None,
            b"value",
            0,
            8,
        )
        inbox = _InboxRecord(
            "mé",
            1,
            "orders.debug_state",
            "κ",
            b"abc",
            1,
        )

        self.assertEqual(
            _outbox_logical_bytes(append),
            160
            + len("mé".encode())
            + len("orders.debug_state".encode())
            + len("κ".encode())
            + 3,
        )
        self.assertEqual(
            _outbox_logical_bytes(latest),
            160
            + len("latest".encode())
            + len("state".encode())
            + len("source-α".encode())
            + 5,
        )
        self.assertEqual(
            _inbox_logical_bytes(inbox),
            128
            + len("mé".encode())
            + len("orders.debug_state".encode())
            + len("κ".encode())
            + 3,
        )

    def test_new_rows_persist_exact_literal_logical_byte_contract(self) -> None:
        path = self._root / "persisted-sizes.sqlite3"
        policies = (
            TopicDeliveryPolicy.commands(
                "orders",
                max_items=8,
                max_bytes=1024 * 1024,
                ttl_seconds=10.0,
            ),
            TopicDeliveryPolicy.latest(
                "state",
                max_sources=8,
                max_bytes=1024 * 1024,
                ttl_seconds=10.0,
                max_inbox_items=8,
            ),
        )
        config = DeliveryConfig(
            path,
            max_outbox_items=8,
            max_inbox_items=8,
            max_storage_bytes=1024 * 1024,
            recovery_batch_size=8,
            message_ttl_seconds=10.0,
            topic_policies=policies,
        )
        append = _OutboxRecord(
            "mé",
            "orders",
            "append",
            None,
            1,
            "κ",
            b"abc",
            0,
            64,
        )
        latest = _OutboxRecord(
            "latest",
            "state",
            "latest",
            "source-α",
            1,
            None,
            b"value",
            0,
            64,
        )
        inbox = _InboxRecord(
            "in-é",
            1,
            "orders",
            "corr",
            b"inbox",
            1,
        )
        rejection = _InboxRecord(
            "reject-é",
            1,
            "forbidden",
            None,
            b"not-retained",
            1,
        )
        expected = {
            "mé": 160
            + len("mé".encode())
            + len("orders".encode())
            + len("κ".encode())
            + len(b"abc"),
            "latest": 160
            + len("latest".encode())
            + len("state".encode())
            + len("source-α".encode())
            + len(b"value"),
            "in-é": 128
            + len("in-é".encode())
            + len("orders".encode())
            + len("corr".encode())
            + len(b"inbox"),
            "reject-é": 128
            + len("reject-é".encode())
            + len("forbidden".encode()),
        }

        journal = _DeliveryJournal(config)
        journal.insert_outbox(
            append,
            created_at=1.0,
            expires_at=11.0,
            now=1.0,
            policy=policies[0],
        )
        journal.insert_outbox(
            latest,
            created_at=1.0,
            expires_at=11.0,
            now=1.0,
            policy=policies[1],
        )
        journal.record_inbox(
            inbox,
            created_at=1.0,
            expires_at=11.0,
            now=1.0,
            policy=policies[0],
        )
        self.assertTrue(
            journal.mark_inbox_outcome(
                "in-é",
                status="terminal",
                reason="rejected",
                now=2.0,
                retention_seconds=10.0,
            )
        )
        journal.record_terminal_rejection(
            rejection,
            reason="unconfigured",
            now=1.0,
        )
        self.assertEqual(journal.stats().logical_bytes, sum(expected.values()))
        journal.close()

        with closing(sqlite3.connect(path)) as connection:
            stored = dict(
                connection.execute(
                    """
                    SELECT message_id, size_bytes FROM outbox
                    UNION ALL
                    SELECT message_id, size_bytes FROM inbox
                    """
                ).fetchall()
            )
        self.assertEqual(stored, expected)

    def test_shipped_v1_schema_migrates_sizes_attempts_and_exact_indexes(
        self,
    ) -> None:
        path = self._root / "v1.sqlite3"
        self._create_v1(path)
        journal = _DeliveryJournal(self._config(path))
        journal.close()

        with closing(sqlite3.connect(path)) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 2)
            outbox = connection.execute(
                """
                SELECT semantics, source_key, max_attempts, size_bytes
                FROM outbox WHERE message_id = 'out-α'
                """
            ).fetchone()
            inbox = connection.execute(
                """
                SELECT status, rejection_only, size_bytes
                FROM inbox WHERE message_id = 'in-α'
                """
            ).fetchone()
            indexes = {
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex%'
                    """
                )
            }
            latest_index_sql = str(
                connection.execute(
                    """
                    SELECT sql FROM sqlite_master
                    WHERE name = 'outbox_latest_source'
                    """
                ).fetchone()[0]
            )

        self.assertEqual(outbox[:3], ("append", None, 7))
        self.assertEqual(
            outbox[3],
            160
            + len("out-α".encode())
            + len("orders".encode())
            + len("corr-α".encode())
            + len(b"payload"),
        )
        self.assertEqual(inbox[:2], ("acked", 0))
        self.assertEqual(
            inbox[2],
            128
            + len("in-α".encode())
            + len("orders".encode())
            + len("corr-in".encode())
            + len(b"inbox"),
        )
        self.assertEqual(
            indexes,
            {
                "inbox_ack_due",
                "inbox_recovery",
                "inbox_status",
                "outbox_due",
                "outbox_latest_source",
                "outbox_recovery",
                "outbox_replay",
            },
        )
        self.assertTrue(all("_v2_" not in name for name in indexes))
        self.assertIn("WHERE semantics = 'latest'", latest_index_sql)

    def test_v2_schema_corruption_is_not_silently_repaired(self) -> None:
        corruptions = {
            "missing-table": "DROP TABLE inbox",
            "wrong-latest-predicate": """
                DROP INDEX outbox_latest_source;
                CREATE UNIQUE INDEX outbox_latest_source
                ON outbox(channel, source_key) WHERE semantics = 'append';
            """,
            "unexpected-trigger": """
                CREATE TRIGGER mutate_outbox AFTER INSERT ON outbox
                BEGIN
                    DELETE FROM outbox WHERE message_id = NEW.message_id;
                END;
            """,
        }
        for name, script in corruptions.items():
            with self.subTest(corruption=name):
                path = self._root / f"{name}.sqlite3"
                journal = _DeliveryJournal(self._config(path))
                journal.close()
                with closing(sqlite3.connect(path)) as connection:
                    connection.executescript(script)
                    self.assertEqual(
                        connection.execute("PRAGMA quick_check(1)").fetchone()[0],
                        "ok",
                    )
                with self.assertRaisesRegex(
                    _JournalError,
                    "contract|unexpected|predicate",
                ):
                    _DeliveryJournal(self._config(path))

    def test_fresh_schema_initialization_rolls_back_as_one_unit(self) -> None:
        path = self._root / "atomic-initialization.sqlite3"
        with closing(
            sqlite3.connect(path, isolation_level=None)
        ) as connection:
            connection.execute("PRAGMA page_size=512")
            connection.execute("PRAGMA max_page_count=1")
            with self.assertRaises(sqlite3.DatabaseError):
                _initialize_schema(
                    connection,
                    config=self._config(path),
                    recovery_now=1.0,
                )
            objects = connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type IN ('table', 'index')
                  AND name NOT LIKE 'sqlite_%'
                """
            ).fetchall()
            self.assertEqual(objects, [])
            self.assertEqual(
                connection.execute("PRAGMA user_version").fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute("PRAGMA application_id").fetchone()[0],
                0,
            )

            connection.execute("PRAGMA max_page_count=4096")
            _initialize_schema(
                connection,
                config=self._config(path),
                recovery_now=1.0,
            )
            self.assertEqual(
                connection.execute("PRAGMA user_version").fetchone()[0],
                2,
            )
            self.assertEqual(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM journal_metadata
                    WHERE key IN ('message_namespace', 'message_sequence')
                    """
                ).fetchone()[0],
                2,
            )

    def test_v2_message_id_metadata_is_canonical_and_finitely_bounded(
        self,
    ) -> None:
        for name, value in (
            ("message_namespace", "not-a-canonical-namespace"),
            ("message_sequence", str(_MAX_MESSAGE_SEQUENCE + 1)),
            ("message_sequence", "0001"),
        ):
            with self.subTest(name=name, value=value):
                path = self._root / f"metadata-{name}-{len(value)}.sqlite3"
                journal = _DeliveryJournal(self._config(path))
                journal.close()
                with closing(sqlite3.connect(path)) as connection:
                    connection.execute(
                        """
                        UPDATE journal_metadata SET value = ?
                        WHERE key = ?
                        """,
                        (value, name),
                    )
                    connection.commit()
                with self.assertRaisesRegex(_JournalError, "invalid"):
                    _DeliveryJournal(self._config(path))

        exhausted_path = self._root / "metadata-exhausted.sqlite3"
        journal = _DeliveryJournal(self._config(exhausted_path))
        journal.close()
        with closing(sqlite3.connect(exhausted_path)) as connection:
            connection.execute(
                """
                UPDATE journal_metadata SET value = ?
                WHERE key = 'message_sequence'
                """,
                (str(_MAX_MESSAGE_SEQUENCE),),
            )
            connection.commit()
        exhausted = _DeliveryJournal(self._config(exhausted_path))
        with self.assertRaisesRegex(_JournalFull, "identifier space"):
            exhausted.next_message_id()
        exhausted.close()

    def test_v1_migration_rolls_back_when_lowered_page_cap_cannot_fit(
        self,
    ) -> None:
        path = self._root / "page-capped-v1.sqlite3"
        self._create_v1(path)
        with closing(sqlite3.connect(path)) as connection:
            index = 0
            while int(connection.execute("PRAGMA page_count").fetchone()[0]) < 16:
                connection.execute(
                    """
                    INSERT INTO outbox VALUES (?, 1, 'orders', NULL, ?, 1, 100,
                                               0, 1, NULL, 1)
                    """,
                    (f"filler-{index}", b"x" * 4096),
                )
                index += 1
            connection.commit()
            connection.execute("VACUUM")
            page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
            pages = int(connection.execute("PRAGMA page_count").fetchone()[0])
            self.assertEqual(
                connection.execute("PRAGMA freelist_count").fetchone()[0],
                0,
            )
        config = self._config(
            path,
            max_storage_bytes=max(64 * 1024, pages * page_size),
            max_outbox_items=128,
            max_inbox_items=128,
            recovery_batch_size=64,
        )

        with self.assertRaises(_JournalError):
            _DeliveryJournal(config)

        with closing(sqlite3.connect(path)) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 1)
            columns = {
                str(row[1])
                for row in connection.execute("PRAGMA table_info(outbox)")
            }
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
        self.assertNotIn("semantics", columns)
        self.assertNotIn("outbox_v2", tables)
        self.assertNotIn("inbox_v2", tables)

    def test_response_attempt_budget_is_not_replenished_by_duplicates_or_restart(
        self,
    ) -> None:
        path = self._root / "responses.sqlite3"
        config = self._config(path, max_ack_attempts=2)
        journal = _DeliveryJournal(config)
        record = _InboxRecord("terminal-1", 1, "orders", None, b"payload", 1)
        first = journal.record_terminal_rejection(
            record,
            reason="terminal",
            now=1.0,
        )
        self.assertIs(first.disposition, _TerminalRejectionDisposition.NEW)
        duplicate = journal.record_terminal_rejection(
            record,
            reason="terminal",
            now=2.0,
        )
        self.assertIs(
            duplicate.disposition,
            _TerminalRejectionDisposition.DUPLICATE,
        )
        for attempt in range(2):
            due = journal.due_responses(
                2.0 + attempt,
                limit=4,
                max_attempts=2,
            )
            self.assertEqual(len(due), 1)
            journal.mark_response_attempt(
                "terminal-1",
                next_attempt_at=3.0 + attempt,
                max_attempts=2,
            )
        self.assertFalse(
            journal.schedule_response_now(
                "terminal-1",
                4.0,
                max_attempts=2,
            )
        )
        journal.close()

        reopened = _DeliveryJournal(config)
        self.assertEqual(
            reopened.due_responses(5.0, limit=4, max_attempts=2),
            (),
        )
        after_restart = reopened.record_terminal_rejection(
            record,
            reason="terminal",
            now=5.0,
        )
        self.assertIs(
            after_restart.disposition,
            _TerminalRejectionDisposition.DUPLICATE,
        )
        self.assertEqual(
            reopened.due_responses(6.0, limit=4, max_attempts=2),
            (),
        )
        compacted = reopened.compact(12.0, limit=4)
        self.assertEqual(compacted.deleted_items, 1)
        self.assertEqual(reopened.stats().terminal_inbox_items, 0)
        reopened.close()

    def test_recovery_queries_use_covering_keyset_indexes_without_temp_sort(
        self,
    ) -> None:
        path = self._root / "plans.sqlite3"
        journal = _DeliveryJournal(self._config(path))
        journal.close()
        with closing(sqlite3.connect(path)) as connection:
            plans = (
                connection.execute(
                    """
                    EXPLAIN QUERY PLAN
                    SELECT channel, semantics, source_key, size_bytes
                    FROM outbox INDEXED BY outbox_recovery
                    ORDER BY channel, created_at, message_id
                    """
                ).fetchall(),
                connection.execute(
                    """
                    EXPLAIN QUERY PLAN
                    SELECT message_id, channel, source_key, correlation_id,
                           attempts, created_at
                    FROM outbox INDEXED BY outbox_replay
                    WHERE (created_at, message_id) > (-1, '')
                    ORDER BY created_at, message_id LIMIT 4
                    """
                ).fetchall(),
                connection.execute(
                    """
                    EXPLAIN QUERY PLAN
                    SELECT channel, NULL, rejection_only, size_bytes
                    FROM inbox INDEXED BY inbox_recovery
                    ORDER BY channel, created_at, message_id
                    """
                ).fetchall(),
            )
        details = tuple(str(row[-1]) for plan in plans for row in plan)
        self.assertFalse(
            any("TEMP B-TREE" in detail.upper() for detail in details),
            details,
        )
        self.assertTrue(any("outbox_recovery" in detail for detail in details))
        self.assertTrue(any("outbox_replay" in detail for detail in details))
        self.assertTrue(any("inbox_recovery" in detail for detail in details))

    def test_recovery_fails_closed_on_lowered_attempt_and_ttl_policy(
        self,
    ) -> None:
        for lowered_policy in (
            TopicDeliveryPolicy.commands(
                "orders",
                max_items=64,
                max_bytes=1024 * 1024,
                ttl_seconds=10.0,
                max_attempts=6,
            ),
            TopicDeliveryPolicy.commands(
                "orders",
                max_items=64,
                max_bytes=1024 * 1024,
                ttl_seconds=9.0,
                max_attempts=7,
            ),
        ):
            with self.subTest(policy=lowered_policy):
                path = self._root / f"lowered-{lowered_policy.max_attempts}.sqlite3"
                initial = self._config(path)
                journal = _DeliveryJournal(initial)
                policy = initial.topic_policies[0]
                journal.insert_outbox(
                    _OutboxRecord(
                        "retained",
                        "orders",
                        "append",
                        None,
                        1,
                        None,
                        b"value",
                        0,
                        policy.max_attempts,
                    ),
                    created_at=1.0,
                    expires_at=11.0,
                    now=1.0,
                    policy=policy,
                )
                journal.close()
                lowered = DeliveryConfig(
                    path,
                    max_outbox_items=64,
                    max_inbox_items=64,
                    max_storage_bytes=1024 * 1024,
                    recovery_batch_size=8,
                    message_ttl_seconds=10.0,
                    max_delivery_attempts=7,
                    topic_policies=(lowered_policy,),
                )
                reopened = _DeliveryJournal(lowered)
                with self.assertRaisesRegex(
                    _JournalFull,
                    "max_attempts|lifetime",
                ):
                    reopened.validate_recovery(
                        {"orders": lowered_policy},
                        max_transport_payload_bytes=1 << 30,
                        recovery_now=1.0,
                    )
                reopened.close()

    def test_recovery_rejects_valid_sqlite_with_corrupt_logical_size(self) -> None:
        path = self._root / "semantic-corruption.sqlite3"
        config = self._config(path)
        journal = _DeliveryJournal(config)
        policy = config.topic_policies[0]
        journal.insert_outbox(
            _OutboxRecord(
                "corrupt-me",
                "orders",
                "append",
                None,
                1,
                None,
                b"value",
                0,
                policy.max_attempts,
            ),
            created_at=1.0,
            expires_at=11.0,
            now=1.0,
            policy=policy,
        )
        journal.close()
        with closing(sqlite3.connect(path)) as connection:
            connection.execute(
                "UPDATE outbox SET size_bytes = 160 WHERE message_id = 'corrupt-me'"
            )
            connection.commit()
            self.assertEqual(
                connection.execute("PRAGMA quick_check(1)").fetchone()[0],
                "ok",
            )

        reopened = _DeliveryJournal(config)
        with self.assertRaisesRegex(_JournalError, "inconsistent logical size"):
            reopened.validate_recovery(
                {"orders": policy},
                max_transport_payload_bytes=1 << 30,
                recovery_now=1.0,
            )
        reopened.close()

    def test_recovery_validates_wire_fields_and_complete_transport_frame(
        self,
    ) -> None:
        corruptions = {
            "frame-kind": (
                "UPDATE outbox SET frame_kind = 99",
                1 << 30,
                "frame kind",
            ),
            "message-id": (
                """
                UPDATE outbox
                SET message_id = printf('%0130d', 1),
                    size_bytes = size_bytes + 122
                """,
                1 << 30,
                "message_id",
            ),
            "transport-frame": (
                "SELECT 1",
                32,
                "transport limit",
            ),
        }
        for name, (statement, transport_limit, expected) in corruptions.items():
            with self.subTest(corruption=name):
                path = self._root / f"wire-{name}.sqlite3"
                config = self._config(path)
                journal = _DeliveryJournal(config)
                policy = config.topic_policies[0]
                journal.insert_outbox(
                    _OutboxRecord(
                        "wire-row",
                        "orders",
                        "append",
                        None,
                        1,
                        None,
                        b"value",
                        0,
                        policy.max_attempts,
                    ),
                    created_at=1.0,
                    expires_at=11.0,
                    now=1.0,
                    policy=policy,
                )
                journal.close()
                with closing(sqlite3.connect(path)) as connection:
                    if name == "frame-kind":
                        connection.execute(
                            "PRAGMA ignore_check_constraints=ON"
                        )
                    connection.execute(statement)
                    if name == "frame-kind":
                        connection.execute(
                            "PRAGMA ignore_check_constraints=OFF"
                        )
                    connection.commit()
                if name == "frame-kind":
                    with self.assertRaisesRegex(_JournalError, "integrity"):
                        _DeliveryJournal(config)
                    continue
                reopened = _DeliveryJournal(config)
                with self.assertRaisesRegex(_JournalError, expected):
                    reopened.validate_recovery(
                        {"orders": policy},
                        max_transport_payload_bytes=transport_limit,
                        recovery_now=1.0,
                    )
                reopened.close()

    def test_recovery_accepts_subsecond_ttl_at_current_epoch_magnitude(
        self,
    ) -> None:
        path = self._root / "subsecond.sqlite3"
        policy = TopicDeliveryPolicy.commands(
            "orders",
            max_items=8,
            max_bytes=1024 * 1024,
            ttl_seconds=0.1,
        )
        config = DeliveryConfig(
            path,
            max_outbox_items=8,
            max_inbox_items=8,
            max_storage_bytes=1024 * 1024,
            recovery_batch_size=8,
            message_ttl_seconds=0.1,
            topic_policies=(policy,),
        )
        now = 1_790_000_000.123456
        journal = _DeliveryJournal(config)
        journal.insert_outbox(
            _OutboxRecord(
                "subsecond",
                "orders",
                "append",
                None,
                1,
                None,
                b"value",
                0,
                policy.max_attempts,
            ),
            created_at=now,
            expires_at=now + policy.ttl_seconds,
            now=now,
            policy=policy,
        )
        journal.close()

        reopened = _DeliveryJournal(config)
        self.assertEqual(
            reopened.validate_recovery(
                {"orders": policy},
                max_transport_payload_bytes=1 << 20,
                recovery_now=now,
            ),
            (),
        )
        reopened.close()

    def _config(
        self,
        path: Path,
        *,
        max_storage_bytes: int = 1024 * 1024,
        max_outbox_items: int = 64,
        max_inbox_items: int = 64,
        recovery_batch_size: int = 8,
        max_ack_attempts: int = 8,
    ) -> DeliveryConfig:
        return DeliveryConfig(
            path,
            max_outbox_items=max_outbox_items,
            max_inbox_items=max_inbox_items,
            max_storage_bytes=max_storage_bytes,
            recovery_batch_size=recovery_batch_size,
            message_ttl_seconds=10.0,
            dedupe_retention_seconds=10.0,
            max_delivery_attempts=8,
            max_ack_attempts=max_ack_attempts,
            topic_policies=(
                TopicDeliveryPolicy.commands(
                    "orders",
                    max_items=max_outbox_items,
                    max_bytes=max_storage_bytes,
                    ttl_seconds=10.0,
                    max_attempts=7,
                ),
            ),
        )

    def _create_v1(self, path: Path) -> None:
        now = time.time()
        with closing(sqlite3.connect(path)) as connection:
            connection.executescript(_V1_SCHEMA)
            connection.execute(f"PRAGMA application_id={_APPLICATION_ID}")
            connection.execute("PRAGMA user_version=1")
            connection.execute(
                """
                INSERT INTO outbox VALUES (
                    'out-α', 1, 'orders', 'corr-α', X'7061796c6f6164',
                    ?, ?, 2, ?, 'stale', 1
                )
                """,
                (now, now + 10.0, now),
            )
            connection.execute(
                """
                INSERT INTO inbox VALUES (
                    'in-α', 1, 'orders', 'corr-in', X'696e626f78',
                    3, 'acked', ?, ?, 1, ?, 0, 1
                )
                """,
                (now, now + 10.0, now),
            )
            connection.commit()


if __name__ == "__main__":
    unittest.main()
