from __future__ import annotations

import gc
import os
import sqlite3
import tempfile
import time
import unittest
import weakref
from collections import Counter
from pathlib import Path
from queue import Queue
from threading import Event, Thread, current_thread, enumerate as enumerate_threads

from manyfold.architecture._transport_delivery_events import (
    DeliveryEvent,
    DeliveryEventKind,
    DeliveryHealth,
)
from manyfold.architecture._transport_delivery_journal import _DeliveryJournal
from manyfold.architecture._transport_delivery_journal_errors import (
    _JournalError,
)
from manyfold.architecture._transport_delivery_protocol import (
    DELIVERY_CHANNEL,
    _DeliveryOperation,
    _encode_delivery_frame,
)
from manyfold.architecture._transport_delivery_records import _OutboxRecord
from manyfold.architecture.transport import (
    FrameKind,
    NodeIdentity,
    ReconnectPolicy,
    TcpAddress,
    TcpTransport,
    TransportConfig,
    TransportMessage,
    TransportSecurity,
)
from manyfold.architecture.transport_delivery import (
    DeliveryClosed,
    DeliveryConfig,
    DeliveryError,
    DurableDelivery,
    TopicDeliveryPolicy,
)


class TransportDeliveryLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory()
        self._root = Path(self._temporary_directory.name)
        self._deliveries: list[DurableDelivery] = []
        self._transports: list[TcpTransport] = []

    def tearDown(self) -> None:
        for delivery in reversed(self._deliveries):
            delivery.close()
        for transport in reversed(self._transports):
            transport.close()
        self._temporary_directory.cleanup()

    def test_volatile_tokens_never_create_outbound_or_inbound_rows(self) -> None:
        raw_sender, receiver_transport = self._transport_pair()
        delivery = self._track_delivery(
            DurableDelivery(
                receiver_transport,
                self._config(self._root / "volatile.sqlite3"),
            )
        )
        for topic in (
            "heart.audio.samples",
            "DEBUG/INPUT/EVENTS",
            "scene-render:tick",
        ):
            with self.subTest(outbound_topic=topic):
                with self.assertRaisesRegex(ValueError, "volatile V1"):
                    delivery.send(
                        TransportMessage(FrameKind.PUBSUB, topic, b"hot"),
                        message_id=f"outbound-{topic}",
                    )
        for index in range(48):
            topic = (
                "heart.audio.samples"
                if index % 2 == 0
                else "Scene.Render.Tick"
            )
            raw_sender.send(
                TransportMessage(
                    FrameKind.PUBSUB,
                    DELIVERY_CHANNEL,
                    _encode_delivery_frame(
                        _DeliveryOperation.DATA,
                        f"volatile-{index}",
                        frame_kind=int(FrameKind.PUBSUB),
                        channel=topic,
                        payload=b"hot",
                        delivery_attempt=1,
                    ),
                )
            )
        health = self._wait_health(
            delivery,
            lambda snapshot: snapshot.terminal_drops == 48,
        )

        self.assertEqual(health.outbox_items, 0)
        self.assertEqual(health.pending_inbox_items, 0)
        self.assertEqual(health.acked_inbox_items, 0)
        self.assertEqual(health.terminal_inbox_items, 0)
        self.assertEqual(health.expired_inbox_items, 0)
        self.assertEqual(health.logical_storage_bytes, 0)
        self.assertLessEqual(
            delivery._sender._controls.qsize(),
            delivery.config.receive_queue_limit,
        )

    def test_padded_wire_metadata_fails_closed_without_creating_rows(self) -> None:
        raw_sender, receiver_transport = self._transport_pair()
        delivery = self._track_delivery(
            DurableDelivery(
                receiver_transport,
                self._config(self._root / "padded.sqlite3"),
            )
        )
        raw_sender.send(
            TransportMessage(
                FrameKind.PUBSUB,
                DELIVERY_CHANNEL,
                _encode_delivery_frame(
                    _DeliveryOperation.DATA,
                    "padded-1",
                    frame_kind=int(FrameKind.PUBSUB),
                    channel=" events ",
                    payload=b"value",
                    delivery_attempt=1,
                ),
            )
        )
        health = self._wait_health(delivery, lambda snapshot: snapshot.closed)

        self.assertEqual(health.pending_inbox_items, 0)
        self.assertEqual(health.logical_storage_bytes, 0)
        self.assertRegex(health.last_error or "", "surrounding whitespace")

    def test_startup_observer_can_read_exact_pre_worker_health(self) -> None:
        path = self._root / "startup-observer.sqlite3"
        policy = TopicDeliveryPolicy.commands(
            "events",
            max_items=1,
            max_bytes=1024 * 1024,
            ttl_seconds=5.0,
            soft_limit_ratio=0.5,
        )
        config = self._config(path, policy=policy, max_items=2)
        journal = _DeliveryJournal(config)
        now = time.time()
        journal.insert_outbox(
            _OutboxRecord(
                "replayed-1",
                "events",
                "append",
                None,
                int(FrameKind.PUBSUB),
                None,
                b"value",
                0,
                policy.max_attempts,
            ),
            created_at=now,
            expires_at=now + 5.0,
            now=now,
            policy=policy,
        )
        journal.close()
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "startup-observer"),
                TcpAddress("127.0.0.1", 9),
                config=_transport_config(),
                expected_peer_node_id="missing",
            )
        )
        observed: list[tuple[DeliveryEventKind, DeliveryHealth]] = []
        delivery = DurableDelivery.__new__(DurableDelivery)

        def observer(event: DeliveryEvent) -> None:
            if event.kind in {
                DeliveryEventKind.WATERMARK_RECOVERED,
                DeliveryEventKind.REPLAYED,
            }:
                observed.append((event.kind, delivery.health()))

        delivery.__init__(transport, config, observer=observer)
        self._track_delivery(delivery)

        self.assertTrue(observed)
        self.assertEqual(observed[-1][0], DeliveryEventKind.REPLAYED)
        for _, health in observed:
            self.assertEqual(health.outbox_items, 1)
            self.assertEqual(health.queued_deliveries, 0)
            self.assertEqual(health.inflight_deliveries, 0)
            self.assertIsNone(health.last_error)

    def test_graceful_drain_does_not_consume_worker_shutdown_budget(self) -> None:
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "graceful-budget"),
                TcpAddress("127.0.0.1", 9),
                config=_transport_config(),
                expected_peer_node_id="missing",
            )
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                self._config(
                    self._root / "graceful.sqlite3",
                    worker_join_timeout_seconds=0.15,
                ),
            )
        )
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"retained"),
            message_id="retained",
        )
        started = time.monotonic()
        delivery.close(graceful_timeout=0.2)
        elapsed = time.monotonic() - started

        self.assertGreaterEqual(elapsed, 0.18)
        self.assertLess(elapsed, 0.5)
        self.assertFalse(delivery._sender.is_alive())
        self.assertFalse(delivery._receiver.is_alive())

    def test_unconfigured_conflict_preserves_authoritative_inflight_row(
        self,
    ) -> None:
        raw_sender, receiver_transport = self._transport_pair()
        delivery = self._track_delivery(
            DurableDelivery(
                receiver_transport,
                self._config(self._root / "conflict.sqlite3"),
            )
        )
        raw_sender.send(
            self._data_frame("same-id", "events", b"authoritative")
        )
        received = delivery.receive(timeout=2.0)
        raw_sender.send(
            self._data_frame("same-id", "unknown.command", b"conflict")
        )
        health = self._wait_health(
            delivery,
            lambda snapshot: snapshot.terminal_drops == 1,
        )

        self.assertEqual(received.message.payload, b"authoritative")
        self.assertEqual(health.inflight_deliveries, 1)
        self.assertEqual(health.pending_inbox_items, 1)
        self.assertEqual(health.terminal_inbox_items, 0)
        delivery.ack(received.message_id)
        self.assertEqual(delivery.health().acked_inbox_items, 1)

    def test_receive_handoff_cannot_expose_a_concurrently_expired_row(
        self,
    ) -> None:
        sender_transport, receiver_transport = self._transport_pair()
        sender = self._track_delivery(
            DurableDelivery(
                sender_transport,
                self._config(self._root / "expiry-sender.sqlite3"),
            )
        )
        receiver = self._track_delivery(
            DurableDelivery(
                receiver_transport,
                self._config(self._root / "expiry-receiver.sqlite3"),
            )
        )
        sender.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"queued"),
            message_id="queued-expiry",
        )
        self._wait_health(
            receiver,
            lambda snapshot: snapshot.queued_deliveries == 1,
        )
        result: Queue[BaseException] = Queue()
        with receiver._runtime.transition():
            worker = Thread(
                target=lambda: self._capture_receive(receiver, result),
            )
            worker.start()
            self.assertTrue(
                receiver._runtime.wait_for_transition_waiters(
                    1,
                    timeout=1.0,
                )
            )
            with receiver._journal._transaction() as connection:
                connection.execute(
                    """
                    UPDATE inbox SET created_at = 1.0, expires_at = 2.0
                    WHERE message_id = 'queued-expiry'
                    """
                )
        worker.join(timeout=2.0)

        self.assertFalse(worker.is_alive())
        self.assertIsInstance(result.get_nowait(), TimeoutError)
        health = receiver.health()
        self.assertEqual(health.queued_deliveries, 0)
        self.assertEqual(health.inflight_deliveries, 0)
        self.assertEqual(health.expired_inbox_items, 1)

    def test_commit_failure_is_public_and_does_not_poison_connection(
        self,
    ) -> None:
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "commit-failure"),
                TcpAddress("127.0.0.1", 9),
                config=_transport_config(),
                expected_peer_node_id="missing",
            )
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                self._config(self._root / "commit.sqlite3"),
            )
        )

        def deny_commit(
            action: int,
            detail: str | None,
            unused_database: str | None,
            unused_trigger: str | None,
            unused_source: str | None,
        ) -> int:
            del unused_database, unused_trigger, unused_source
            if action == sqlite3.SQLITE_TRANSACTION and detail == "COMMIT":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        delivery._journal._connection.set_authorizer(deny_commit)
        with self.assertRaises(DeliveryError) as raised:
            delivery.send(
                TransportMessage(FrameKind.PUBSUB, "events", b"value"),
                message_id="commit-failure",
            )
        self.assertIsNotNone(raised.exception.__cause__)
        self.assertFalse(delivery._journal._connection.in_transaction)
        delivery._journal._connection.set_authorizer(None)
        self.assertEqual(delivery.health().outbox_items, 0)
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"value"),
            message_id="after-rollback",
        )
        self.assertEqual(delivery.health().outbox_items, 1)

    def test_failed_final_stats_still_releases_owner_and_close_is_retryable(
        self,
    ) -> None:
        path = self._root / "close-read-failure.sqlite3"
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "close-read-failure"),
                TcpAddress("127.0.0.1", 9),
                config=_transport_config(),
                expected_peer_node_id="missing",
            )
        )
        delivery = self._track_delivery(
            DurableDelivery(transport, self._config(path))
        )

        def deny_reads(
            action: int,
            unused_detail: str | None,
            unused_database: str | None,
            unused_trigger: str | None,
            unused_source: str | None,
        ) -> int:
            del (
                unused_detail,
                unused_database,
                unused_trigger,
                unused_source,
            )
            return (
                sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_SELECT
                else sqlite3.SQLITE_OK
            )

        delivery._journal._connection.set_authorizer(deny_reads)
        try:
            with self.assertRaises(DeliveryError):
                delivery.close()
            self.assertTrue(delivery._journal_released)
        finally:
            if delivery._journal.has_open_connection():
                delivery._journal._connection.set_authorizer(None)
            delivery.close()
        replacement = _DeliveryJournal(self._config(path))
        replacement.close()

    def test_sender_fatal_wakes_blocked_receive_and_flush_without_spin(
        self,
    ) -> None:
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "sender-fatal"),
                TcpAddress("127.0.0.1", 9),
                config=_transport_config(),
                expected_peer_node_id="missing",
            )
        )
        config = self._config(self._root / "sender-fatal.sqlite3")
        delivery = self._track_delivery(DurableDelivery(transport, config))
        receive_result: Queue[BaseException] = Queue()
        started = Event()
        receiver_waiter = Thread(
            target=lambda: self._capture_blocked_receive(
                delivery,
                started,
                receive_result,
            )
        )
        receiver_waiter.start()
        self.assertTrue(started.wait(timeout=1.0))
        policy = config.topic_policies[0]
        with delivery._runtime.transition():
            now = time.time()
            delivery._journal.insert_outbox(
                _OutboxRecord(
                    "fatal-row",
                    "events",
                    "append",
                    None,
                    int(FrameKind.PUBSUB),
                    None,
                    b"value",
                    0,
                    policy.max_attempts,
                ),
                created_at=now,
                expires_at=now + 5.0,
                now=now,
                policy=policy,
            )

            def deny_sender_reads(
                action: int,
                unused_detail: str | None,
                unused_database: str | None,
                unused_trigger: str | None,
                unused_source: str | None,
            ) -> int:
                del (
                    unused_detail,
                    unused_database,
                    unused_trigger,
                    unused_source,
                )
                if (
                    action == sqlite3.SQLITE_SELECT
                    and current_thread().name.endswith("-sender")
                ):
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            delivery._journal._connection.set_authorizer(deny_sender_reads)
        health = self._wait_health(delivery, lambda snapshot: snapshot.closed)
        receiver_waiter.join(timeout=1.0)

        self.assertFalse(receiver_waiter.is_alive())
        self.assertIsInstance(receive_result.get_nowait(), DeliveryClosed)
        with self.assertRaises(DeliveryClosed):
            delivery.flush()
        self.assertRegex(health.last_error or "", "not authorized")
        delivery.close()
        self.assertFalse(delivery._sender.is_alive())
        self.assertFalse(delivery._receiver.is_alive())

    def test_real_transport_queue_pressure_spends_no_network_attempt(
        self,
    ) -> None:
        server = self._track_transport(
            TcpTransport.listen(
                NodeIdentity("cluster", "pressure-receiver"),
                config=_transport_config(
                    outbound_queue_limit=1,
                    inbound_queue_limit=1,
                    max_payload_bytes=65536,
                ),
                expected_peer_node_id="pressure-sender",
            )
        )
        client = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "pressure-sender"),
                server.address,
                config=_transport_config(
                    outbound_queue_limit=1,
                    inbound_queue_limit=1,
                    max_payload_bytes=65536,
                ),
                expected_peer_node_id="pressure-receiver",
            )
        )
        self.assertTrue(server.wait_until_connected(timeout=2.0))
        self.assertTrue(client.wait_until_connected(timeout=2.0))
        pressure = Event()
        observed: Queue[DeliveryEvent] = Queue()

        def observer(event: DeliveryEvent) -> None:
            if (
                event.kind is DeliveryEventKind.RETRY_SCHEDULED
                and event.local_pressure_count
            ):
                observed.put(event)
                pressure.set()

        policy = TopicDeliveryPolicy.commands(
            "events",
            max_items=128,
            max_bytes=12 * 1024 * 1024,
            ttl_seconds=5.0,
        )
        config = DeliveryConfig(
            self._root / "real-pressure.sqlite3",
            max_outbox_items=128,
            max_inbox_items=128,
            max_storage_bytes=16 * 1024 * 1024,
            receive_queue_limit=8,
            work_batch_size=32,
            recovery_batch_size=64,
            max_message_bytes=60_000,
            message_ttl_seconds=5.0,
            retry_initial_seconds=0.5,
            retry_max_seconds=0.5,
            topic_policies=(policy,),
        )
        delivery = self._track_delivery(
            DurableDelivery(client, config, observer=observer)
        )
        for index in range(128):
            delivery.send(
                TransportMessage(
                    FrameKind.PUBSUB,
                    "events",
                    bytes((index % 251,)) * 60_000,
                ),
                message_id=f"pressure-{index:03d}",
            )
            if pressure.is_set():
                break
        self.assertTrue(pressure.wait(timeout=3.0))
        event = observed.get_nowait()
        with delivery._runtime.transition():
            row = delivery._journal._connection.execute(
                """
                SELECT attempts, created_at, next_attempt_at
                FROM outbox WHERE message_id = ?
                """,
                (event.message_id,),
            ).fetchone()

        self.assertIsNotNone(row)
        self.assertEqual(int(row[0]), event.attempt)
        self.assertGreater(float(row[2]), float(row[1]))
        self.assertLessEqual(float(row[2]) - time.time(), 0.5)
        self.assertGreaterEqual(event.local_pressure_count, 1)

    def test_data_and_response_retry_budgets_exhaust_exactly(self) -> None:
        data_transport, _raw_receiver = self._transport_pair()
        data_policy = TopicDeliveryPolicy.commands(
            "events",
            max_items=4,
            max_bytes=1024 * 1024,
            ttl_seconds=5.0,
            max_attempts=2,
        )
        data_config = DeliveryConfig(
            self._root / "data-exhaustion.sqlite3",
            max_outbox_items=4,
            max_inbox_items=4,
            max_storage_bytes=1024 * 1024,
            recovery_batch_size=4,
            max_message_bytes=4096,
            message_ttl_seconds=5.0,
            retry_initial_seconds=0.02,
            retry_max_seconds=0.02,
            max_delivery_attempts=2,
            topic_policies=(data_policy,),
        )
        data_delivery = self._track_delivery(
            DurableDelivery(data_transport, data_config)
        )
        data_delivery.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"value"),
            message_id="exhaust-data",
        )
        data_health = self._wait_health(
            data_delivery,
            lambda snapshot: snapshot.retry_exhausted == 1,
            timeout=3.0,
        )
        self.assertEqual(data_health.outbox_items, 0)
        self.assertEqual(data_health.frames_sent, 2)

        raw_sender, receiver_transport = self._transport_pair()
        response_config = self._config(
            self._root / "response-exhaustion.sqlite3",
            max_ack_attempts=2,
        )
        response_delivery = self._track_delivery(
            DurableDelivery(receiver_transport, response_config)
        )
        raw_sender.send(
            self._data_frame("exhaust-response", "events", b"value")
        )
        received = response_delivery.receive(timeout=2.0)
        response_delivery.ack(received.message_id)
        response_health = self._wait_health(
            response_delivery,
            lambda snapshot: snapshot.ack_retry_exhausted == 1,
            timeout=3.0,
        )
        self.assertEqual(response_health.acked_inbox_items, 1)
        self.assertEqual(response_health.frames_sent, 2)

    def test_close_serializes_with_admitted_send_and_other_close(self) -> None:
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "close-race"),
                TcpAddress("127.0.0.1", 9),
                config=_transport_config(),
                expected_peer_node_id="missing",
            )
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                self._config(self._root / "close-race.sqlite3"),
            )
        )
        send_errors: Queue[BaseException] = Queue()
        close_errors: Queue[BaseException] = Queue()
        with delivery._runtime.transition():
            sender = Thread(
                target=lambda: self._capture_send(delivery, send_errors)
            )
            sender.start()
            self.assertTrue(
                delivery._runtime.wait_for_transition_waiters(1, timeout=1.0)
            )
            closers = [
                Thread(
                    target=lambda: self._capture_close(
                        delivery,
                        close_errors,
                    )
                )
                for _ in range(2)
            ]
            for closer in closers:
                closer.start()
        sender.join(timeout=2.0)
        for closer in closers:
            closer.join(timeout=2.0)

        self.assertFalse(sender.is_alive())
        self.assertTrue(all(not closer.is_alive() for closer in closers))
        self.assertTrue(send_errors.empty())
        self.assertTrue(close_errors.empty())
        self.assertTrue(delivery.health().closed)

    def test_special_journal_paths_fail_before_runtime_ownership(self) -> None:
        baseline_threads = self._delivery_threads()
        with self.assertRaisesRegex(ValueError, "not :memory:"):
            DeliveryConfig(Path(":memory:"))
        with self.assertRaisesRegex(ValueError, "regular file"):
            DeliveryConfig(self._root)
        fifo = self._root / "journal.fifo"
        os.mkfifo(fifo)
        with self.assertRaisesRegex(ValueError, "special target"):
            DeliveryConfig(fifo)
        relative = DeliveryConfig(
            Path("relative-durable.sqlite3"),
            topic_policies=(),
        )

        self.assertTrue(relative.journal_path.is_absolute())
        self.assertEqual(self._delivery_threads(), baseline_threads)

    def test_symlink_alias_cannot_acquire_a_second_journal_owner(self) -> None:
        canonical = self._root / "canonical.sqlite3"
        config = self._config(canonical)
        owner = _DeliveryJournal(config)
        alias = self._root / "alias.sqlite3"
        alias.symlink_to(canonical)
        alias_config = self._config(alias)

        self.assertEqual(alias_config.journal_path, canonical.resolve())
        try:
            with self.assertRaisesRegex(_JournalError, "already owned"):
                _DeliveryJournal(alias_config)
        finally:
            owner.close()

    def test_hard_link_added_after_open_cannot_acquire_an_alias_owner(
        self,
    ) -> None:
        canonical = self._root / "hard-link-canonical.sqlite3"
        owner = _DeliveryJournal(self._config(canonical))
        alias = self._root / "hard-link-alias.sqlite3"
        alias_config = self._config(alias)
        try:
            os.link(canonical, alias)
            with self.assertRaisesRegex(_JournalError, "hard-linked"):
                _DeliveryJournal(alias_config)
            with self.assertRaisesRegex(ValueError, "hard-linked"):
                self._config(alias)
        finally:
            owner.close()

    @unittest.skipIf(os.name == "nt", "POSIX unlink semantics required")
    def test_unlinked_original_keeps_inode_alias_exclusively_owned(self) -> None:
        canonical = self._root / "unlinked-canonical.sqlite3"
        owner = _DeliveryJournal(self._config(canonical))
        alias = self._root / "unlinked-alias.sqlite3"
        try:
            os.link(canonical, alias)
            canonical.unlink()
            alias_config = self._config(alias)
            self.assertEqual(alias.stat().st_nlink, 1)
            with self.assertRaisesRegex(_JournalError, "already owned"):
                _DeliveryJournal(alias_config)
        finally:
            owner.close()

    def test_health_wait_returns_only_newer_state_and_rejects_closed_current(
        self,
    ) -> None:
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "health-wait"),
                TcpAddress("127.0.0.1", 9),
                config=_transport_config(),
                expected_peer_node_id="missing",
            )
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                self._config(self._root / "health-wait.sqlite3"),
            )
        )
        before = delivery.health()
        delivery.close()
        closed = delivery.wait_for_health_change(
            before.generation,
            timeout=0.2,
        )
        self.assertGreater(closed.generation, before.generation)
        started = time.monotonic()
        with self.assertRaises(DeliveryClosed):
            delivery.wait_for_health_change(
                closed.generation,
                timeout=0.2,
            )
        self.assertLess(time.monotonic() - started, 0.1)

    def test_natural_disconnected_close_has_no_thread_fd_or_callback_tail(
        self,
    ) -> None:
        baseline_threads = Counter(self._delivery_threads())
        baseline_fds = len(os.listdir("/dev/fd"))
        for index in range(4):
            transport = TcpTransport.connect(
                NodeIdentity("cluster", f"natural-close-{index}"),
                TcpAddress("127.0.0.1", 9),
                config=_transport_config(),
                expected_peer_node_id="missing",
            )
            delivery = DurableDelivery(
                transport,
                self._config(
                    self._root / f"natural-close-{index}.sqlite3"
                ),
            )
            receiver_ref = weakref.ref(delivery._receiver)
            sender_ref = weakref.ref(delivery._sender)
            journal_ref = weakref.ref(delivery._journal)
            delivery.close()
            self.assertIsNone(delivery._runtime._unavailable_waker)
            self.assertEqual(delivery._sender._controls.qsize(), 0)
            transport.close()
            del delivery, transport
            gc.collect()
            self.assertIsNone(receiver_ref())
            self.assertIsNone(sender_ref())
            self.assertIsNone(journal_ref())
        deadline = time.monotonic() + 2.0
        while (
            Counter(self._delivery_threads()) != baseline_threads
            or len(os.listdir("/dev/fd")) > baseline_fds
        ) and time.monotonic() < deadline:
            time.sleep(0.01)

        self.assertEqual(
            Counter(self._delivery_threads()),
            baseline_threads,
        )
        self.assertLessEqual(len(os.listdir("/dev/fd")), baseline_fds)

    def _transport_pair(self) -> tuple[TcpTransport, TcpTransport]:
        server = self._track_transport(
            TcpTransport.listen(
                NodeIdentity("cluster", "receiver", "receiver-1"),
                config=_transport_config(),
                expected_peer_node_id="sender",
            )
        )
        client = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "sender", "sender-1"),
                server.address,
                config=_transport_config(),
                expected_peer_node_id="receiver",
            )
        )
        self.assertTrue(server.wait_until_connected(timeout=2.0))
        self.assertTrue(client.wait_until_connected(timeout=2.0))
        return client, server

    def _config(
        self,
        path: Path,
        *,
        policy: TopicDeliveryPolicy | None = None,
        max_items: int = 16,
        max_ack_attempts: int = 64,
        worker_join_timeout_seconds: float = 2.0,
    ) -> DeliveryConfig:
        resolved_policy = policy or TopicDeliveryPolicy.commands(
            "events",
            max_items=max_items,
            max_bytes=1024 * 1024,
            ttl_seconds=5.0,
        )
        return DeliveryConfig(
            path,
            max_outbox_items=max_items,
            max_inbox_items=max_items,
            max_storage_bytes=1024 * 1024,
            receive_queue_limit=8,
            recovery_batch_size=min(8, max_items),
            max_message_bytes=4096,
            message_ttl_seconds=5.0,
            retry_initial_seconds=0.02,
            retry_max_seconds=0.1,
            max_ack_attempts=max_ack_attempts,
            worker_join_timeout_seconds=worker_join_timeout_seconds,
            topic_policies=(resolved_policy,),
        )

    def _data_frame(
        self,
        message_id: str,
        channel: str,
        payload: bytes,
    ) -> TransportMessage:
        return TransportMessage(
            FrameKind.PUBSUB,
            DELIVERY_CHANNEL,
            _encode_delivery_frame(
                _DeliveryOperation.DATA,
                message_id,
                frame_kind=int(FrameKind.PUBSUB),
                channel=channel,
                payload=payload,
                delivery_attempt=1,
            ),
        )

    def _wait_health(
        self,
        delivery: DurableDelivery,
        predicate: object,
        *,
        timeout: float = 2.0,
    ) -> DeliveryHealth:
        deadline = time.monotonic() + timeout
        health = delivery.health()
        while not predicate(health):
            health = delivery.wait_for_health_change(
                health.generation,
                timeout=max(0.0, deadline - time.monotonic()),
            )
        return health

    def _capture_receive(
        self,
        delivery: DurableDelivery,
        result: Queue[BaseException],
    ) -> None:
        try:
            delivery.receive(timeout=0.5)
        except BaseException as error:
            result.put(error)

    def _capture_blocked_receive(
        self,
        delivery: DurableDelivery,
        started: Event,
        result: Queue[BaseException],
    ) -> None:
        started.set()
        try:
            delivery.receive()
        except BaseException as error:
            result.put(error)

    def _capture_send(
        self,
        delivery: DurableDelivery,
        errors: Queue[BaseException],
    ) -> None:
        try:
            delivery.send(
                TransportMessage(FrameKind.PUBSUB, "events", b"value"),
                message_id="admitted-send",
            )
        except BaseException as error:
            errors.put(error)

    def _capture_close(
        self,
        delivery: DurableDelivery,
        errors: Queue[BaseException],
    ) -> None:
        try:
            delivery.close()
        except BaseException as error:
            errors.put(error)

    def _track_delivery(self, delivery: DurableDelivery) -> DurableDelivery:
        self._deliveries.append(delivery)
        return delivery

    def _track_transport(self, transport: TcpTransport) -> TcpTransport:
        self._transports.append(transport)
        return transport

    def _delivery_threads(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                thread.name
                for thread in enumerate_threads()
                if thread.name.startswith("manyfold-delivery-")
            )
        )


def _transport_config(
    *,
    outbound_queue_limit: int = 64,
    inbound_queue_limit: int = 64,
    max_payload_bytes: int = 65536,
) -> TransportConfig:
    return TransportConfig(
        security=TransportSecurity.insecure_local_development(),
        outbound_queue_limit=outbound_queue_limit,
        inbound_queue_limit=inbound_queue_limit,
        max_payload_bytes=max_payload_bytes,
        connect_timeout=0.1,
        handshake_timeout=0.5,
        heartbeat_interval=0.05,
        peer_timeout=0.5,
        reconnect=ReconnectPolicy(0.02, 1.5, 0.1),
    )
