from __future__ import annotations

import os
import random
import sqlite3
import tempfile
import time
import tracemalloc
import unittest
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from pathlib import Path
from typing import TypeVar

from manyfold.architecture._transport_delivery_journal import _DeliveryJournal
from manyfold.architecture._transport_delivery_journal_errors import (
    _JournalError,
    _JournalFull,
)
from manyfold.architecture._transport_delivery_policy import (
    DeliveryConfig,
    TopicDeliveryPolicy,
)
from manyfold.architecture._transport_delivery_records import (
    _InboxRecord,
    _OutboxRecord,
    _ReplayCursor,
)
from manyfold.architecture._transport_delivery_schema import _initialize_schema
from manyfold.architecture.transport import (
    FrameKind,
    NodeIdentity,
    ReconnectPolicy,
    TcpAddress,
    TcpTransport,
    TransportConfig,
    TransportSecurity,
)
from manyfold.architecture.transport_delivery import (
    DeliveryStorageFull,
    DurableDelivery,
)

from tests.test_transport_delivery_journal_persistence import (
    _APPLICATION_ID,
    _V1_SCHEMA,
)

_ResultT = TypeVar("_ResultT")


class TransportDeliveryRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory()
        self._root = Path(self._temporary_directory.name)

    def tearDown(self) -> None:
        self._temporary_directory.cleanup()

    def test_v1_padded_text_fails_atomically_for_python_strip_whitespace(
        self,
    ) -> None:
        for index, padded in enumerate(
            (" order-1", "order-1\t", "\norder-1", "\u2003order-1")
        ):
            with self.subTest(padded=repr(padded)):
                path = self._root / f"v1-padded-{index}.sqlite3"
                self._create_v1(path, message_id=padded)

                with self.assertRaisesRegex(_JournalError, "non-canonical"):
                    _DeliveryJournal(self._config(path))
                with closing(sqlite3.connect(path)) as connection:
                    self.assertEqual(
                        connection.execute(
                            "PRAGMA user_version"
                        ).fetchone()[0],
                        1,
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT message_id FROM outbox"
                        ).fetchone()[0],
                        padded,
                    )
                    self.assertNotIn(
                        "outbox_v2",
                        {
                            str(row[0])
                            for row in connection.execute(
                                """
                                SELECT name FROM sqlite_master
                                WHERE type = 'table'
                                """
                            )
                        },
                    )

    def test_v2_unicode_padded_text_fails_bounded_recovery(self) -> None:
        path = self._root / "v2-unicode-padding.sqlite3"
        config = self._config(path)
        journal = _DeliveryJournal(config)
        self._insert(journal, config.topic_policies[0], "canonical", b"value")
        journal.close()
        with closing(sqlite3.connect(path)) as connection:
            connection.execute(
                """
                UPDATE outbox
                SET message_id = ?, size_bytes = size_bytes + 3
                WHERE message_id = 'canonical'
                """,
                ("\u2003canonical",),
            )
            connection.commit()
            self.assertEqual(
                connection.execute("PRAGMA quick_check(1)").fetchone()[0],
                "ok",
            )
        reopened = _DeliveryJournal(config)
        with self.assertRaisesRegex(_JournalError, "non-canonical"):
            reopened.validate_recovery(
                {"orders": config.topic_policies[0]},
                max_transport_payload_bytes=1 << 20,
                recovery_now=time.time(),
            )
        reopened.close()

    def test_truncated_and_non_database_files_fail_closed(self) -> None:
        for name, data in (
            ("random", b"not a sqlite database"),
            ("truncated", None),
        ):
            with self.subTest(name=name):
                path = self._root / f"{name}.sqlite3"
                if data is None:
                    journal = _DeliveryJournal(self._config(path))
                    self._insert(
                        journal,
                        journal._config.topic_policies[0],
                        "retained",
                        b"x" * 4096,
                    )
                    journal.close()
                    original = path.read_bytes()
                    path.write_bytes(original[: len(original) // 2])
                else:
                    path.write_bytes(data)
                with self.assertRaises(_JournalError):
                    _DeliveryJournal(self._config(path))

    def test_recovery_rejects_lowered_item_and_topic_byte_bounds(self) -> None:
        for case in ("items", "bytes"):
            with self.subTest(case=case):
                path = self._root / f"lowered-{case}.sqlite3"
                initial = self._config(path, max_items=4, max_bytes=4096)
                journal = _DeliveryJournal(initial)
                self._insert(
                    journal,
                    initial.topic_policies[0],
                    "first",
                    b"x" * 200,
                )
                if case == "items":
                    self._insert(
                        journal,
                        initial.topic_policies[0],
                        "second",
                        b"y",
                    )
                journal.close()
                lowered = self._config(
                    path,
                    max_items=1 if case == "items" else 4,
                    max_bytes=180 if case == "bytes" else 4096,
                )
                reopened = _DeliveryJournal(lowered)
                with self.assertRaisesRegex(
                    _JournalFull,
                    "item|logical byte",
                ):
                    reopened.validate_recovery(
                        {"orders": lowered.topic_policies[0]},
                        max_transport_payload_bytes=1 << 20,
                        recovery_now=time.time(),
                    )
                reopened.close()

    def test_v2_current_ack_and_payload_limits_are_capacity_drift(
        self,
    ) -> None:
        for case in ("ack-attempts", "payload"):
            with self.subTest(case=case):
                path = self._root / f"v2-current-{case}.sqlite3"
                initial = self._config(
                    path,
                    max_ack_attempts=4,
                    max_message_bytes=8192,
                )
                journal = _DeliveryJournal(initial)
                policy = initial.topic_policies[0]
                if case == "payload":
                    self._insert(journal, policy, "retained", b"x" * 4096)
                else:
                    now = time.time()
                    journal.record_inbox(
                        _InboxRecord(
                            "retained",
                            int(FrameKind.PUBSUB),
                            "orders",
                            None,
                            b"value",
                            1,
                        ),
                        created_at=now,
                        expires_at=now + policy.ttl_seconds,
                        now=now,
                        policy=policy,
                    )
                    journal.mark_inbox_outcome(
                        "retained",
                        status="acked",
                        reason=None,
                        now=now,
                        retention_seconds=5.0,
                    )
                journal.close()
                if case == "ack-attempts":
                    with closing(sqlite3.connect(path)) as connection:
                        connection.execute(
                            """
                            UPDATE inbox SET ack_attempts = 3
                            WHERE message_id = 'retained'
                            """
                        )
                        connection.commit()
                current = self._config(
                    path,
                    max_ack_attempts=2 if case == "ack-attempts" else 4,
                    max_message_bytes=1024 if case == "payload" else 8192,
                )
                reopened = _DeliveryJournal(current)
                with self.assertRaisesRegex(
                    _JournalFull,
                    "ack_attempts|payload",
                ):
                    reopened.validate_recovery(
                        {"orders": current.topic_policies[0]},
                        max_transport_payload_bytes=1 << 20,
                        recovery_now=time.time(),
                    )
                reopened.close()

    def test_public_startup_types_v1_v2_current_limits_as_storage_full(
        self,
    ) -> None:
        for schema in ("v1", "v2"):
            for limit in ("ack-attempts", "payload"):
                with self.subTest(schema=schema, limit=limit):
                    path = self._root / f"public-{schema}-{limit}.sqlite3"
                    if schema == "v1":
                        self._create_v1_capacity_rows(
                            path,
                            outbox_rows=1 if limit == "payload" else 0,
                            inbox_rows=1 if limit == "ack-attempts" else 0,
                            attempts=0,
                            ack_attempts=3,
                        )
                        if limit == "payload":
                            with closing(sqlite3.connect(path)) as connection:
                                connection.execute(
                                    """
                                    UPDATE outbox SET payload = zeroblob(4096)
                                    """
                                )
                                connection.commit()
                    else:
                        initial = self._config(
                            path,
                            max_ack_attempts=4,
                            max_message_bytes=8192,
                        )
                        journal = _DeliveryJournal(initial)
                        policy = initial.topic_policies[0]
                        if limit == "payload":
                            self._insert(
                                journal,
                                policy,
                                "retained",
                                b"x" * 4096,
                            )
                        else:
                            now = time.time()
                            journal.record_inbox(
                                _InboxRecord(
                                    "retained",
                                    int(FrameKind.PUBSUB),
                                    "orders",
                                    None,
                                    b"value",
                                    1,
                                ),
                                created_at=now,
                                expires_at=now + policy.ttl_seconds,
                                now=now,
                                policy=policy,
                            )
                            journal.mark_inbox_outcome(
                                "retained",
                                status="acked",
                                reason=None,
                                now=now,
                                retention_seconds=5.0,
                            )
                        journal.close()
                        if limit == "ack-attempts":
                            with closing(sqlite3.connect(path)) as connection:
                                connection.execute(
                                    """
                                    UPDATE inbox SET ack_attempts = 3
                                    WHERE message_id = 'retained'
                                    """
                                )
                                connection.commit()
                    current = self._config(
                        path,
                        max_ack_attempts=(
                            2 if limit == "ack-attempts" else 4
                        ),
                        max_message_bytes=(
                            1024 if limit == "payload" else 8192
                        ),
                    )
                    transport = _disconnected_transport(
                        f"public-{schema}-{limit}"
                    )
                    try:
                        with self.assertRaisesRegex(
                            DeliveryStorageFull,
                            "ack|payload|message",
                        ):
                            DurableDelivery(transport, current)
                    finally:
                        transport.close()
                    if schema == "v1":
                        with closing(sqlite3.connect(path)) as connection:
                            self.assertEqual(
                                connection.execute(
                                    "PRAGMA user_version"
                                ).fetchone()[0],
                                1,
                            )

    def test_expired_v2_rows_compact_before_current_runtime_limits(
        self,
    ) -> None:
        path = self._root / "expired-before-current-limits.sqlite3"
        initial = self._config(
            path,
            max_ack_attempts=4,
            max_message_bytes=8192,
        )
        journal = _DeliveryJournal(initial)
        policy = initial.topic_policies[0]
        expired_at = time.time() - 5.0
        created_at = expired_at - 5.0
        journal.insert_outbox(
            _OutboxRecord(
                "expired-outbox",
                "orders",
                "append",
                None,
                int(FrameKind.PUBSUB),
                None,
                b"x" * 1500,
                0,
                policy.max_attempts,
            ),
            created_at=created_at,
            expires_at=expired_at,
            now=created_at,
            policy=policy,
        )
        for message_id, status in (
            ("expired-pending", "pending"),
            ("expired-acked", "acked"),
        ):
            journal.record_inbox(
                _InboxRecord(
                    message_id,
                    int(FrameKind.PUBSUB),
                    "orders",
                    None,
                    b"x" * 1500,
                    1,
                ),
                created_at=created_at,
                expires_at=expired_at,
                now=created_at,
                policy=policy,
            )
            if status == "acked":
                journal.mark_inbox_outcome(
                    message_id,
                    status="acked",
                    reason=None,
                    now=created_at,
                    retention_seconds=5.0,
                )
        journal.close()
        with closing(sqlite3.connect(path)) as connection:
            connection.execute(
                """
                UPDATE inbox SET ack_attempts = 3
                WHERE message_id = 'expired-acked'
                """
            )
            connection.commit()
        current = self._config(
            path,
            max_ack_attempts=2,
            max_message_bytes=1024,
        )
        transport = _disconnected_transport(
            "expired-current-limits",
            max_payload_bytes=1400,
        )
        delivery = DurableDelivery(transport, current)
        try:
            health = delivery.health()
            self.assertEqual(health.outbox_items, 0)
            self.assertEqual(health.pending_inbox_items, 0)
            self.assertEqual(health.acked_inbox_items, 0)
            self.assertEqual(health.expired_inbox_items, 1)
        finally:
            delivery.close()
            transport.close()

    def test_expired_v1_rows_migrate_and_compact_before_current_caps(
        self,
    ) -> None:
        path = self._root / "v1-expired-current-cap.sqlite3"
        self._create_v1_capacity_rows(
            path,
            outbox_rows=1,
            inbox_rows=0,
            attempts=0,
            ack_attempts=0,
        )
        with closing(sqlite3.connect(path)) as connection:
            connection.execute(
                """
                UPDATE outbox
                SET payload = zeroblob(4096), created_at = 0, expires_at = 1
                """
            )
            connection.commit()
        current = self._config(path, max_message_bytes=1024)

        journal = _DeliveryJournal(current)
        self.assertEqual(journal.stats().outbox_items, 0)
        journal.close()
        with closing(sqlite3.connect(path)) as connection:
            self.assertEqual(
                connection.execute("PRAGMA user_version").fetchone()[0],
                2,
            )

    def test_large_v1_migration_and_replay_keep_python_memory_batched(
        self,
    ) -> None:
        path = self._root / "large-v1.sqlite3"
        row_count = 128
        payload = b"x" * (32 * 1024)
        now = time.time()
        with closing(sqlite3.connect(path)) as connection:
            connection.executescript(_V1_SCHEMA)
            connection.execute(f"PRAGMA application_id={_APPLICATION_ID}")
            connection.execute("PRAGMA user_version=1")
            connection.executemany(
                """
                INSERT INTO outbox VALUES (
                    ?, 1, 'orders', NULL, ?, ?, ?, 0, ?, NULL, 1
                )
                """,
                (
                    (
                        f"row-{index:04d}",
                        payload,
                        now,
                        now + 10.0,
                        now,
                    )
                    for index in range(row_count)
                ),
            )
            connection.commit()
        config = self._config(
            path,
            max_items=row_count,
            max_bytes=16 * 1024 * 1024,
            max_storage_bytes=16 * 1024 * 1024,
            recovery_batch_size=4,
        )
        with _measured_journal(config) as (journal, migration_peak):
            self.assertLess(migration_peak, 2 * 1024 * 1024)
            self.assertEqual(journal.stats().outbox_items, row_count)

            def replay_all() -> int:
                cursor: _ReplayCursor | None = None
                replayed = 0
                while True:
                    batch = journal.outbox_replay_batch(cursor, limit=4)
                    if not batch:
                        return replayed
                    replayed += len(batch)
                    cursor = batch[-1].cursor

            replayed, replay_peak = _measure_traced_peak(replay_all)
            self.assertEqual(replayed, row_count)
            self.assertLess(replay_peak, 512 * 1024)

    def test_batched_memory_measurement_ignores_active_prior_peak(self) -> None:
        tracing_was_active = tracemalloc.is_tracing()
        if not tracing_was_active:
            tracemalloc.start()
        try:
            prior_peak_allocation = bytearray(4 * 1024 * 1024)
            self.assertGreater(
                tracemalloc.get_traced_memory()[1],
                2 * 1024 * 1024,
            )
            del prior_peak_allocation

            measured_value, measured_peak = _measure_traced_peak(
                lambda: len(bytearray(1024))
            )

            self.assertEqual(measured_value, 1024)
            self.assertLess(measured_peak, 512 * 1024)
            self.assertTrue(tracemalloc.is_tracing())
        finally:
            if not tracing_was_active and tracemalloc.is_tracing():
                tracemalloc.stop()

    def test_measured_journal_closes_after_assertion_failure(self) -> None:
        config = self._config(self._root / "measurement-failure.sqlite3")
        baseline_fds = _fd_count()
        holder = None
        with self.assertRaisesRegex(AssertionError, "intentional failure"):
            with _measured_journal(config) as (journal, _peak):
                holder = journal._owner_lock._process
                raise AssertionError("intentional failure")
        if holder is None:
            raise AssertionError("journal holder was not created")
        self.assertEqual(holder.wait(timeout=1.0), 0)

        reopened = _DeliveryJournal(config)
        reopened.close()
        if baseline_fds is not None:
            self.assertTrue(
                _wait_for(
                    lambda: _fd_count() is not None
                    and _fd_count() <= baseline_fds,
                    timeout=1.0,
                )
            )

    def test_randomized_insertions_never_exceed_advertised_caps(self) -> None:
        generator = random.Random(280)
        path = self._root / "randomized-caps.sqlite3"
        config = self._config(path, max_items=16, max_bytes=4096)
        journal = _DeliveryJournal(config)
        policy = config.topic_policies[0]
        accepted = 0
        for index in range(128):
            payload = generator.randbytes(generator.randint(0, 128))
            try:
                self._insert(
                    journal,
                    policy,
                    f"message-{index}",
                    payload,
                )
            except _JournalFull:
                pass
            else:
                accepted += 1
            stats = journal.stats()
            topic = journal.topic_stats("orders")
            self.assertLessEqual(stats.outbox_items, config.max_outbox_items)
            self.assertLessEqual(stats.logical_bytes, config.max_storage_bytes)
            self.assertLessEqual(topic.outbox_items, policy.max_items)
            self.assertLessEqual(topic.logical_bytes, policy.max_bytes)
        journal.close()

        self.assertGreater(accepted, 0)
        self.assertLessEqual(accepted, policy.max_items)

    def test_v2_integral_fields_reject_real_storage_classes(self) -> None:
        cases = (
            ("outbox", "frame_kind"),
            ("outbox", "attempts"),
            ("outbox", "max_attempts"),
            ("outbox", "size_bytes"),
            ("inbox", "frame_kind"),
            ("inbox", "delivery_attempt"),
            ("inbox", "ack_attempts"),
            ("inbox", "ack_confirmed"),
            ("inbox", "rejection_only"),
            ("inbox", "size_bytes"),
        )
        for table, field in cases:
            with self.subTest(table=table, field=field):
                path = self._root / f"real-{table}-{field}.sqlite3"
                config = self._config(path)
                journal = _DeliveryJournal(config)
                policy = config.topic_policies[0]
                if table == "outbox":
                    self._insert(journal, policy, "row", b"value")
                else:
                    journal.record_inbox(
                        _InboxRecord(
                            "row",
                            int(FrameKind.PUBSUB),
                            "orders",
                            None,
                            b"value",
                            1,
                        ),
                        created_at=1.0,
                        expires_at=10.0,
                        now=1.0,
                        policy=policy,
                    )
                journal.close()
                with closing(sqlite3.connect(path)) as connection:
                    connection.execute(
                        "PRAGMA ignore_check_constraints=ON"
                    )
                    connection.execute(
                        f"UPDATE {table} SET {field} = 0.5"
                    )
                    connection.execute(
                        "PRAGMA ignore_check_constraints=OFF"
                    )
                    connection.commit()
                with self.assertRaisesRegex(_JournalError, "integrity"):
                    _DeliveryJournal(config)

    def test_v1_non_utf8_encoding_fails_before_migration(self) -> None:
        path = self._root / "utf16-v1.sqlite3"
        with closing(sqlite3.connect(path)) as connection:
            connection.execute("PRAGMA encoding='UTF-16le'")
            connection.executescript(_V1_SCHEMA)
            connection.execute(f"PRAGMA application_id={_APPLICATION_ID}")
            connection.execute("PRAGMA user_version=1")
            connection.execute(
                """
                INSERT INTO outbox VALUES (
                    'mé', 1, 'orders', NULL, X'76',
                    1, 100, 0, 1, NULL, 1
                )
                """
            )
            connection.commit()

        with self.assertRaisesRegex(_JournalError, "UTF-8"):
            _DeliveryJournal(self._config(path))
        with closing(sqlite3.connect(path)) as connection:
            self.assertEqual(
                connection.execute("PRAGMA user_version").fetchone()[0],
                1,
            )
            self.assertEqual(
                connection.execute("PRAGMA encoding").fetchone()[0],
                "UTF-16le",
            )
            self.assertEqual(
                connection.execute("SELECT message_id FROM outbox").fetchone()[0],
                "mé",
            )

    def test_v1_current_capacity_mismatches_are_typed_and_atomic(self) -> None:
        cases = (
            ("outbox-items", 2, 0, 0, 0, 1, 64, 64, 64),
            ("inbox-items", 0, 2, 0, 0, 1, 64, 64, 64),
            ("topic-attempts", 1, 0, 7, 0, 8, 64, 64, 6),
            ("global-attempts", 1, 0, 7, 0, 8, 6, 64, 6),
            ("ack-attempts", 0, 1, 0, 7, 8, 64, 6, 64),
        )
        for (
            name,
            outbox_rows,
            inbox_rows,
            attempts,
            ack_attempts,
            max_items,
            max_delivery_attempts,
            max_ack_attempts,
            topic_attempts,
        ) in cases:
            with self.subTest(name=name):
                path = self._root / f"v1-cap-{name}.sqlite3"
                self._create_v1_capacity_rows(
                    path,
                    outbox_rows=outbox_rows,
                    inbox_rows=inbox_rows,
                    attempts=attempts,
                    ack_attempts=ack_attempts,
                )
                config = self._config(
                    path,
                    max_items=max_items,
                    max_delivery_attempts=max_delivery_attempts,
                    max_ack_attempts=max_ack_attempts,
                    topic_attempts=topic_attempts,
                )
                with self.assertRaisesRegex(_JournalFull, "current"):
                    _DeliveryJournal(config)
                self._assert_v1_unchanged(
                    path,
                    outbox_rows=outbox_rows,
                    inbox_rows=inbox_rows,
                )

        logical_path = self._root / "v1-cap-logical.sqlite3"
        self._create_v1_capacity_rows(
            logical_path,
            outbox_rows=1,
            inbox_rows=0,
            attempts=0,
            ack_attempts=0,
        )
        with closing(sqlite3.connect(logical_path)) as connection:
            connection.execute(
                "UPDATE outbox SET payload = zeroblob(65536)"
            )
            connection.commit()
            with self.assertRaisesRegex(_JournalFull, "max_storage_bytes"):
                _initialize_schema(
                    connection,
                    config=self._config(
                        logical_path,
                        max_bytes=65536,
                        max_storage_bytes=65536,
                        max_message_bytes=65536,
                        recovery_batch_size=1,
                    ),
                    recovery_now=time.time(),
                )
        self._assert_v1_unchanged(
            logical_path,
            outbox_rows=1,
            inbox_rows=0,
        )

    def _config(
        self,
        path: Path,
        *,
        max_items: int = 8,
        max_bytes: int = 1024 * 1024,
        max_storage_bytes: int | None = None,
        recovery_batch_size: int = 4,
        max_delivery_attempts: int = 64,
        max_ack_attempts: int = 64,
        topic_attempts: int = 64,
        max_message_bytes: int = 64 * 1024,
    ) -> DeliveryConfig:
        policy = TopicDeliveryPolicy.commands(
            "orders",
            max_items=max_items,
            max_bytes=max_bytes,
            ttl_seconds=10.0,
            max_attempts=topic_attempts,
            max_inbox_items=max_items,
            max_inbox_bytes=max_bytes,
        )
        resolved_storage_bytes = (
            max(1024 * 1024, max_bytes)
            if max_storage_bytes is None
            else max_storage_bytes
        )
        return DeliveryConfig(
            path,
            max_outbox_items=max_items,
            max_inbox_items=max_items,
            max_storage_bytes=resolved_storage_bytes,
            recovery_batch_size=min(recovery_batch_size, max_items),
            max_message_bytes=max_message_bytes,
            message_ttl_seconds=10.0,
            max_delivery_attempts=max_delivery_attempts,
            max_ack_attempts=max_ack_attempts,
            topic_policies=(policy,),
        )

    def _insert(
        self,
        journal: _DeliveryJournal,
        policy: TopicDeliveryPolicy,
        message_id: str,
        payload: bytes,
    ) -> None:
        now = time.time()
        journal.insert_outbox(
            _OutboxRecord(
                message_id,
                policy.topic,
                "append",
                None,
                int(FrameKind.PUBSUB),
                None,
                payload,
                0,
                policy.max_attempts,
            ),
            created_at=now,
            expires_at=now + policy.ttl_seconds,
            now=now,
            policy=policy,
        )

    def _create_v1(self, path: Path, *, message_id: str) -> None:
        with closing(sqlite3.connect(path)) as connection:
            connection.executescript(_V1_SCHEMA)
            connection.execute(f"PRAGMA application_id={_APPLICATION_ID}")
            connection.execute("PRAGMA user_version=1")
            connection.execute(
                """
                INSERT INTO outbox VALUES (
                    ?, 1, 'orders', NULL, X'76',
                    1, 100, 0, 1, NULL, 1
                )
                """,
                (message_id,),
            )
            connection.commit()

    def _create_v1_capacity_rows(
        self,
        path: Path,
        *,
        outbox_rows: int,
        inbox_rows: int,
        attempts: int,
        ack_attempts: int,
    ) -> None:
        now = time.time()
        expires_at = now + 10.0
        with closing(sqlite3.connect(path)) as connection:
            connection.executescript(_V1_SCHEMA)
            connection.execute(f"PRAGMA application_id={_APPLICATION_ID}")
            connection.execute("PRAGMA user_version=1")
            connection.executemany(
                """
                INSERT INTO outbox VALUES (
                    ?, 1, 'orders', NULL, X'76',
                    ?, ?, ?, ?, NULL, 1
                )
                """,
                (
                    (
                        f"out-{index}",
                        now,
                        expires_at,
                        attempts,
                        expires_at,
                    )
                    for index in range(outbox_rows)
                ),
            )
            connection.executemany(
                """
                INSERT INTO inbox VALUES (
                    ?, 1, 'orders', NULL, X'76',
                    1, 'acked', ?, ?, ?, ?, 0, 1
                )
                """,
                (
                    (
                        f"in-{index}",
                        now,
                        expires_at,
                        ack_attempts,
                        now,
                    )
                    for index in range(inbox_rows)
                ),
            )
            connection.commit()

    def _assert_v1_unchanged(
        self,
        path: Path,
        *,
        outbox_rows: int,
        inbox_rows: int,
    ) -> None:
        with closing(sqlite3.connect(path)) as connection:
            self.assertEqual(
                connection.execute("PRAGMA user_version").fetchone()[0],
                1,
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM outbox").fetchone()[0],
                outbox_rows,
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM inbox").fetchone()[0],
                inbox_rows,
            )
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
            self.assertNotIn("outbox_v2", tables)
            self.assertNotIn("inbox_v2", tables)


def _measure_traced_peak(
    operation: Callable[[], _ResultT],
) -> tuple[_ResultT, int]:
    tracing_was_active = tracemalloc.is_tracing()
    if not tracing_was_active:
        tracemalloc.start()
    try:
        tracemalloc.reset_peak()
        baseline_current = tracemalloc.get_traced_memory()[0]
        result = operation()
        peak = tracemalloc.get_traced_memory()[1]
        return result, max(0, peak - baseline_current)
    finally:
        if not tracing_was_active and tracemalloc.is_tracing():
            tracemalloc.stop()


@contextmanager
def _measured_journal(
    config: DeliveryConfig,
) -> Iterator[tuple[_DeliveryJournal, int]]:
    journal, peak = _measure_traced_peak(lambda: _DeliveryJournal(config))
    try:
        yield journal, peak
    finally:
        journal.close()


def _fd_count() -> int | None:
    try:
        return len(os.listdir("/dev/fd"))
    except OSError:
        return None


def _wait_for(predicate: Callable[[], bool], *, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.01)
    return True


def _disconnected_transport(
    node_id: str,
    *,
    max_payload_bytes: int = 64 * 1024,
) -> TcpTransport:
    return TcpTransport.connect(
        NodeIdentity("cluster", node_id),
        TcpAddress("127.0.0.1", 9),
        config=TransportConfig(
            security=TransportSecurity.insecure_local_development(),
            outbound_queue_limit=8,
            inbound_queue_limit=8,
            max_payload_bytes=max_payload_bytes,
            connect_timeout=0.05,
            handshake_timeout=0.1,
            heartbeat_interval=0.05,
            peer_timeout=0.2,
            reconnect=ReconnectPolicy(0.01, 2.0, 0.05),
        ),
        expected_peer_node_id="missing",
    )
