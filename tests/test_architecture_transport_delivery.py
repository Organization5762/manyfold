from __future__ import annotations

import gc
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import weakref
from pathlib import Path
from queue import Queue
from threading import Thread, enumerate as enumerate_threads

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
    DeliveryConflict,
    DeliveryOutcome,
    DeliveryStorageFull,
    DurableDelivery,
    TopicDeliveryPolicy,
)

from tests.test_support import subprocess_test_env


class ArchitectureTransportDeliveryTests(unittest.TestCase):
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

    def test_acknowledged_message_drains_durable_outbox(self) -> None:
        sender, receiver = self._delivery_pair()
        message_id = sender.send(
            TransportMessage(FrameKind.PUBSUB, "orders.created", b"order-7"),
            message_id="order-7",
        )

        received = receiver.receive(timeout=2.0)
        receiver.ack(received.message_id)

        self.assertEqual(message_id, "order-7")
        self.assertEqual(received.message.payload, b"order-7")
        self.assertTrue(sender.flush(timeout=2.0))
        self.assertEqual(sender.health().outbox_items, 0)
        self.assertEqual(receiver.health().acknowledgements, 1)

    def test_nack_releases_inbox_and_redelivers(self) -> None:
        sender, receiver = self._delivery_pair()
        sender.send(
            TransportMessage(
                FrameKind.RPC_REQUEST,
                "cache.get",
                b"k",
                correlation_id="request-1",
            ),
            message_id="request-1",
        )
        first = receiver.receive(timeout=2.0)

        receiver.nack(first.message_id, reason="retryable")
        second = receiver.receive(timeout=2.0)
        receiver.ack(second.message_id)

        self.assertEqual(second.message_id, first.message_id)
        self.assertGreaterEqual(second.delivery_attempt, 2)
        self.assertTrue(sender.flush(timeout=2.0))
        self.assertEqual(receiver.health().negative_acknowledgements, 1)

    def test_unacked_retries_are_suppressed_while_application_is_inflight(
        self,
    ) -> None:
        sender, receiver = self._delivery_pair(retry_initial_seconds=0.02)
        sender.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"one"),
            message_id="duplicate-1",
        )
        received = receiver.receive(timeout=2.0)

        self.assertTrue(
            _wait_for(
                lambda: receiver.health().duplicates_suppressed >= 1,
                timeout=1.0,
            )
        )
        with self.assertRaises(TimeoutError):
            receiver.receive(timeout=0.1)
        receiver.ack(received.message_id)

        self.assertTrue(sender.flush(timeout=2.0))
        self.assertEqual(receiver.health().delivered, 1)

    def test_hard_process_exit_recovers_outbox_and_delivers(self) -> None:
        address = _unused_address()
        journal_path = self._root / "crashed-sender.sqlite3"
        script = """
import os
import sys
from pathlib import Path
from manyfold.architecture.transport import (
    FrameKind, NodeIdentity, ReconnectPolicy, TcpAddress, TcpTransport,
    TransportConfig, TransportMessage, TransportSecurity,
)
from manyfold.architecture.transport_delivery import (
    DeliveryConfig,
    DurableDelivery,
    TopicDeliveryPolicy,
)

transport = TcpTransport.connect(
    NodeIdentity("cluster", "sender", "crashed-process"),
    TcpAddress(sys.argv[1], int(sys.argv[2])),
    config=TransportConfig(
        security=TransportSecurity.insecure_local_development(),
        max_payload_bytes=65536,
        connect_timeout=0.05,
        handshake_timeout=0.2,
        heartbeat_interval=0.05,
        peer_timeout=0.5,
        reconnect=ReconnectPolicy(0.02, 1.5, 0.1),
    ),
    expected_peer_node_id="receiver",
)
delivery = DurableDelivery(
    transport,
    DeliveryConfig(
        Path(sys.argv[3]),
        max_storage_bytes=1024 * 1024,
        max_message_bytes=4096,
        retry_initial_seconds=0.02,
        retry_max_seconds=0.1,
        topic_policies=(
            TopicDeliveryPolicy.commands(
                "recovery",
                max_items=16,
                max_bytes=65536,
                ttl_seconds=5.0,
            ),
        ),
    ),
)
delivery.send(
    TransportMessage(FrameKind.PUBSUB, "recovery", b"persisted"),
    message_id="crash-recovery-1",
)
os._exit(0)
"""
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                script,
                address.host,
                str(address.port),
                str(journal_path),
            ],
            check=False,
            env=subprocess_test_env(),
            timeout=5.0,
        )
        self.assertEqual(completed.returncode, 0)

        server = self._track_transport(
            TcpTransport.listen(
                NodeIdentity("cluster", "receiver", "receiver-1"),
                address,
                config=_transport_config(),
                expected_peer_node_id="sender",
            )
        )
        client = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "sender", "sender-restarted"),
                address,
                config=_transport_config(),
                expected_peer_node_id="receiver",
            )
        )
        receiver = self._track_delivery(
            DurableDelivery(
                server,
                _delivery_config(self._root / "receiver.sqlite3"),
            )
        )
        recovered_sender = self._track_delivery(
            DurableDelivery(client, _delivery_config(journal_path))
        )

        received = receiver.receive(timeout=3.0)
        receiver.ack(received.message_id)

        self.assertEqual(received.message_id, "crash-recovery-1")
        self.assertEqual(received.message.payload, b"persisted")
        self.assertTrue(recovered_sender.flush(timeout=2.0))

    def test_unacknowledged_message_retries_after_live_link_reconnect(self) -> None:
        sender_transport, receiver_transport = self._transport_pair()
        receiver_journal = self._root / "receiver.sqlite3"
        sender = self._track_delivery(
            DurableDelivery(
                sender_transport,
                _delivery_config(self._root / "sender.sqlite3"),
            )
        )
        receiver = self._track_delivery(
            DurableDelivery(
                receiver_transport,
                _delivery_config(receiver_journal),
            )
        )
        address = receiver_transport.address
        receiver.close()
        receiver_transport.close()
        sender.send(
            TransportMessage(FrameKind.PUBSUB, "reconnect", b"waiting"),
            message_id="reconnect-1",
        )

        replacement_transport = self._track_transport(
            TcpTransport.listen(
                NodeIdentity("cluster", "receiver", "receiver-2"),
                address,
                config=_transport_config(),
                expected_peer_node_id="sender",
            )
        )
        replacement = self._track_delivery(
            DurableDelivery(
                replacement_transport,
                _delivery_config(receiver_journal),
            )
        )
        self.assertTrue(sender_transport.wait_until_connected(timeout=2.0))
        try:
            received = replacement.receive(timeout=2.0)
        except TimeoutError as error:
            self.fail(
                "durable reconnect timed out; "
                f"sender_transport={sender_transport.health()!r}; "
                f"sender_delivery={sender.health()!r}; "
                f"replacement_transport={replacement_transport.health()!r}; "
                f"replacement_delivery={replacement.health()!r}; "
                f"cause={error}"
            )
        replacement.ack(received.message_id)

        self.assertEqual(received.message_id, "reconnect-1")
        self.assertTrue(sender.flush(timeout=2.0))
        self.assertGreaterEqual(sender.health().frames_sent, 1)

    def test_acked_inbox_suppresses_same_id_after_reopen(self) -> None:
        sender_transport, receiver_transport = self._transport_pair()
        sender_journal = self._root / "sender.sqlite3"
        receiver_journal = self._root / "receiver.sqlite3"
        sender = self._track_delivery(
            DurableDelivery(sender_transport, _delivery_config(sender_journal))
        )
        receiver = self._track_delivery(
            DurableDelivery(receiver_transport, _delivery_config(receiver_journal))
        )
        message = TransportMessage(FrameKind.PUBSUB, "events", b"value")
        sender.send(message, message_id="stable-1")
        first = receiver.receive(timeout=2.0)
        receiver.ack(first.message_id)
        self.assertTrue(sender.flush(timeout=2.0))

        receiver.close()
        reopened_receiver = self._track_delivery(
            DurableDelivery(
                receiver_transport,
                _delivery_config(receiver_journal),
            )
        )
        sender.send(message, message_id="stable-1")

        self.assertTrue(sender.flush(timeout=2.0))
        with self.assertRaises(TimeoutError):
            reopened_receiver.receive(timeout=0.2)
        self.assertGreaterEqual(
            reopened_receiver.health().duplicates_suppressed,
            1,
        )

    def test_pending_inbox_is_redelivered_after_layer_reopen(self) -> None:
        sender_transport, receiver_transport = self._transport_pair()
        receiver_journal = self._root / "receiver.sqlite3"
        sender = self._track_delivery(
            DurableDelivery(
                sender_transport,
                _delivery_config(self._root / "sender.sqlite3"),
            )
        )
        receiver = self._track_delivery(
            DurableDelivery(receiver_transport, _delivery_config(receiver_journal))
        )
        sender.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"pending"),
            message_id="pending-1",
        )
        first = receiver.receive(timeout=2.0)
        receiver.close()

        reopened = self._track_delivery(
            DurableDelivery(receiver_transport, _delivery_config(receiver_journal))
        )
        recovered = reopened.receive(timeout=1.0)
        reopened.ack(recovered.message_id)

        self.assertEqual(recovered.message_id, first.message_id)
        self.assertTrue(sender.flush(timeout=2.0))

    def test_outbox_item_and_byte_bounds_fail_before_accepting_more(self) -> None:
        address = _unused_address()
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "sender", "sender-1"),
                address,
                config=_transport_config(),
                expected_peer_node_id="receiver",
            )
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                DeliveryConfig(
                    self._root / "bounded.sqlite3",
                    max_outbox_items=1,
                    recovery_batch_size=1,
                    max_storage_bytes=128 * 1024,
                    max_message_bytes=4096,
                    topic_policies=_topic_policies(
                        ttl_seconds=5.0,
                        max_items=1,
                        max_bytes=128 * 1024,
                    ),
                ),
            )
        )
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"first"),
            message_id="first",
        )

        for index in range(32):
            with self.assertRaisesRegex(DeliveryStorageFull, "item limit"):
                delivery.send(
                    TransportMessage(FrameKind.PUBSUB, "events", b"overflow"),
                    message_id=f"overflow-{index}",
                )

        self.assertEqual(delivery.health().outbox_items, 1)
        self.assertLessEqual(
            os.path.getsize(delivery.config.journal_path),
            delivery.config.max_storage_bytes,
        )

    def test_expiry_compacts_unacknowledged_outbox(self) -> None:
        address = _unused_address()
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "sender", "sender-1"),
                address,
                config=_transport_config(),
                expected_peer_node_id="receiver",
            )
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                _delivery_config(
                    self._root / "expiry.sqlite3",
                    message_ttl_seconds=0.1,
                ),
            )
        )
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"expires"),
            message_id="expires-1",
        )

        self.assertTrue(
            _wait_for(
                lambda: delivery.health().expired_outbox == 1,
                timeout=1.5,
            )
        )
        self.assertEqual(delivery.health().outbox_items, 0)

    def test_inbox_expiry_persists_terminal_outcome_and_releases_inflight(
        self,
    ) -> None:
        sender_transport, receiver_transport = self._transport_pair()
        sender = self._track_delivery(
            DurableDelivery(
                sender_transport,
                _delivery_config(
                    self._root / "sender.sqlite3",
                    message_ttl_seconds=5.0,
                ),
            )
        )
        receiver = self._track_delivery(
            DurableDelivery(
                receiver_transport,
                _delivery_config(
                    self._root / "receiver.sqlite3",
                    message_ttl_seconds=0.1,
                    dedupe_retention_seconds=0.2,
                ),
            )
        )
        sender.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"expires"),
            message_id="inflight-expiry-1",
        )
        receiver.receive(timeout=2.0)

        self.assertTrue(
            _wait_for(
                lambda: receiver.health().expired_inbox >= 1,
                timeout=1.5,
            )
        )
        self.assertEqual(receiver.health().inflight_deliveries, 0)
        with self.assertRaises(TimeoutError):
            receiver.receive(timeout=0.2)
        self.assertTrue(sender.flush(timeout=2.0))
        self.assertEqual(sender.health().outbox_items, 0)
        self.assertEqual(sender.health().peer_negative_acknowledgements, 1)

    def test_terminal_outcome_survives_lost_control_frame_and_restart(
        self,
    ) -> None:
        sender_transport, receiver_transport = self._transport_pair()
        sender_path = self._root / "terminal-sender.sqlite3"
        receiver_path = self._root / "terminal-receiver.sqlite3"
        sender = self._track_delivery(
            DurableDelivery(sender_transport, _delivery_config(sender_path))
        )
        receiver = self._track_delivery(
            DurableDelivery(receiver_transport, _delivery_config(receiver_path))
        )
        sender.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"terminal"),
            message_id="terminal-restart",
        )
        received = receiver.receive(timeout=2.0)
        sender_transport.close()
        self.assertTrue(_wait_for(lambda: sender.health().closed, timeout=1.0))
        receiver.nack(
            received.message_id,
            outcome=DeliveryOutcome.terminal("invalid command"),
        )
        self.assertEqual(receiver.health().terminal_drops, 1)
        receiver.close()
        receiver_transport.close()
        sender.close()

        restarted_sender_transport, restarted_receiver_transport = (
            self._transport_pair()
        )
        restarted_sender = self._track_delivery(
            DurableDelivery(
                restarted_sender_transport,
                _delivery_config(sender_path),
            )
        )
        restarted_receiver = self._track_delivery(
            DurableDelivery(
                restarted_receiver_transport,
                _delivery_config(receiver_path),
            )
        )

        self.assertTrue(restarted_sender.flush(timeout=2.0))
        with self.assertRaises(TimeoutError):
            restarted_receiver.receive(timeout=0.2)
        self.assertEqual(restarted_receiver.health().terminal_drops, 0)
        self.assertGreaterEqual(
            restarted_receiver.health().duplicates_suppressed,
            1,
        )

    def test_expired_outcome_survives_lost_control_frame_and_restart(
        self,
    ) -> None:
        sender_transport, receiver_transport = self._transport_pair()
        sender_path = self._root / "expired-sender.sqlite3"
        receiver_path = self._root / "expired-receiver.sqlite3"
        sender = self._track_delivery(
            DurableDelivery(sender_transport, _delivery_config(sender_path))
        )
        receiver = self._track_delivery(
            DurableDelivery(
                receiver_transport,
                _delivery_config(
                    receiver_path,
                    message_ttl_seconds=0.1,
                    dedupe_retention_seconds=2.0,
                ),
            )
        )
        sender.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"expires"),
            message_id="expired-restart",
        )
        receiver.receive(timeout=2.0)
        sender_transport.close()
        self.assertTrue(_wait_for(lambda: sender.health().closed, timeout=1.0))
        self.assertTrue(
            _wait_for(
                lambda: receiver.health().expired_inbox == 1,
                timeout=1.0,
            )
        )
        receiver.close()
        receiver_transport.close()
        sender.close()

        restarted_sender_transport, restarted_receiver_transport = (
            self._transport_pair()
        )
        restarted_sender = self._track_delivery(
            DurableDelivery(
                restarted_sender_transport,
                _delivery_config(sender_path),
            )
        )
        restarted_receiver = self._track_delivery(
            DurableDelivery(
                restarted_receiver_transport,
                _delivery_config(
                    receiver_path,
                    message_ttl_seconds=0.1,
                    dedupe_retention_seconds=2.0,
                ),
            )
        )

        self.assertTrue(restarted_sender.flush(timeout=2.0))
        with self.assertRaises(TimeoutError):
            restarted_receiver.receive(timeout=0.2)
        self.assertEqual(restarted_receiver.health().expired_inbox, 0)
        self.assertGreaterEqual(
            restarted_receiver.health().duplicates_suppressed,
            1,
        )

    def test_logical_byte_bound_rejects_payload_before_disk_growth(self) -> None:
        address = _unused_address()
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "sender", "sender-1"),
                address,
                config=_transport_config(max_payload_bytes=1024 * 1024),
                expected_peer_node_id="receiver",
            )
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                DeliveryConfig(
                    self._root / "byte-bounded.sqlite3",
                    max_outbox_items=8,
                    recovery_batch_size=8,
                    max_storage_bytes=512 * 1024,
                    max_message_bytes=280_000,
                    topic_policies=_topic_policies(
                        ttl_seconds=5.0,
                        max_items=8,
                        max_bytes=512 * 1024,
                    ),
                ),
            )
        )
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"a" * 270_000),
            message_id="large-1",
        )

        with self.assertRaisesRegex(DeliveryStorageFull, "byte"):
            delivery.send(
                TransportMessage(FrameKind.PUBSUB, "events", b"b" * 270_000),
                message_id="large-2",
            )

        self.assertEqual(delivery.health().outbox_items, 1)
        self.assertLessEqual(
            os.path.getsize(delivery.config.journal_path),
            delivery.config.max_storage_bytes,
        )

    def test_stable_id_conflict_and_close_release_resources(self) -> None:
        address = _unused_address()
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "sender", "sender-1"),
                address,
                config=_transport_config(),
                expected_peer_node_id="receiver",
            )
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                _delivery_config(self._root / "lifecycle.sqlite3"),
            )
        )
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"one"),
            message_id="same",
        )
        with self.assertRaisesRegex(DeliveryConflict, "different content"):
            delivery.send(
                TransportMessage(FrameKind.PUBSUB, "events", b"two"),
                message_id="same",
            )

        receive_errors: Queue[BaseException] = Queue()

        def receive_until_closed() -> None:
            try:
                delivery.receive()
            except DeliveryClosed as error:
                receive_errors.put(error)

        blocked_receivers = [
            Thread(target=receive_until_closed),
            Thread(target=receive_until_closed),
        ]
        for receiver in blocked_receivers:
            receiver.start()
        delivery.close()
        for receiver in blocked_receivers:
            receiver.join(timeout=1.0)

        self.assertFalse(delivery._sender.is_alive())
        self.assertFalse(delivery._receiver.is_alive())
        self.assertTrue(all(not receiver.is_alive() for receiver in blocked_receivers))
        self.assertEqual(receive_errors.qsize(), 2)
        self.assertTrue(
            all(
                isinstance(receive_errors.get_nowait(), DeliveryClosed)
                for _ in blocked_receivers
            )
        )
        self.assertTrue(delivery.health().closed)
        with self.assertRaisesRegex(DeliveryClosed, "closed"):
            delivery.send(
                TransportMessage(FrameKind.PUBSUB, "events", b"late")
            )

    def test_final_attempt_ack_wins_before_persisted_response_window(self) -> None:
        sender_transport, receiver_transport = self._transport_pair()
        sender_policy = TopicDeliveryPolicy.commands(
            "events",
            max_items=1,
            max_bytes=1024 * 1024,
            ttl_seconds=5.0,
            max_attempts=1,
        )
        sender = self._track_delivery(
            DurableDelivery(
                sender_transport,
                DeliveryConfig(
                    self._root / "final-attempt-sender.sqlite3",
                    max_outbox_items=1,
                    max_inbox_items=1,
                    max_storage_bytes=1024 * 1024,
                    recovery_batch_size=1,
                    max_message_bytes=4096,
                    message_ttl_seconds=5.0,
                    retry_initial_seconds=0.4,
                    retry_max_seconds=0.4,
                    max_delivery_attempts=1,
                    topic_policies=(sender_policy,),
                ),
            )
        )
        receiver = self._track_delivery(
            DurableDelivery(
                receiver_transport,
                _delivery_config(self._root / "final-attempt-receiver.sqlite3"),
            )
        )
        sender.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"delayed-ack"),
            message_id="final-attempt",
        )
        received = receiver.receive(timeout=2.0)
        with self.assertRaises(DeliveryStorageFull):
            sender.send(
                TransportMessage(FrameKind.PUBSUB, "events", b"must-wait"),
                message_id="second-attempt",
            )
        self.assertEqual(sender.health().outbox_items, 1)
        receiver.ack(received.message_id)

        self.assertTrue(sender.flush(timeout=2.0))
        self.assertEqual(sender.health().retry_exhausted, 0)
        self.assertEqual(sender.health().outbox_items, 0)

    def test_unexpected_transport_close_wakes_waiters_and_flush(self) -> None:
        sender, _ = self._delivery_pair()
        receive_errors: Queue[BaseException] = Queue()

        def wait_for_receive() -> None:
            try:
                sender.receive()
            except BaseException as error:
                receive_errors.put(error)

        blocked = Thread(target=wait_for_receive)
        blocked.start()
        sender.transport.close()
        self.assertTrue(
            _wait_for(lambda: sender.health().closed, timeout=1.0)
        )
        snapshot = sender.health()
        started = time.monotonic()
        with self.assertRaises(DeliveryClosed):
            sender.flush()
        self.assertLess(time.monotonic() - started, 0.2)
        started = time.monotonic()
        with self.assertRaises(DeliveryClosed):
            sender.wait_for_health_change(
                snapshot.generation,
                timeout=1.0,
            )
        self.assertLess(time.monotonic() - started, 0.2)
        blocked.join(timeout=1.0)
        self.assertFalse(blocked.is_alive())
        self.assertIsInstance(receive_errors.get_nowait(), DeliveryClosed)

    def test_close_releases_threads_fds_controls_and_callback_graphs(self) -> None:
        baseline_threads = {
            thread.name
            for thread in enumerate_threads()
            if thread.name.startswith("manyfold-delivery-")
        }
        baseline_fds = len(os.listdir("/dev/fd"))

        class Callback:
            def __call__(self, value: object) -> None:
                del value

        for index in range(5):
            observer = Callback()
            validator = Callback()
            observer_ref = weakref.ref(observer)
            validator_ref = weakref.ref(validator)
            transport = TcpTransport.connect(
                NodeIdentity("cluster", f"tail-{index}", f"tail-{index}"),
                _unused_address(),
                config=_transport_config(),
                expected_peer_node_id="missing",
            )
            delivery = DurableDelivery(
                transport,
                _delivery_config(self._root / f"tail-{index}.sqlite3"),
                observer=observer,
                receive_validator=validator,
            )
            del observer
            del validator
            delivery._runtime.stop.set()
            delivery._sender.wake()
            delivery._receiver.wake_receivers()
            self.assertTrue(delivery._sender.join(1.0))
            self.assertTrue(delivery._receiver.join(1.0))
            delivery._sender.enqueue_retryable("pending-control", "retained")
            self.assertGreater(delivery._sender._controls.qsize(), 0)
            delivery.close()
            transport.close()
            gc.collect()
            self.assertIsNone(observer_ref())
            self.assertIsNone(validator_ref())
            self.assertEqual(delivery._sender._controls.qsize(), 0)

        self.assertTrue(
            _wait_for(
                lambda: {
                    thread.name
                    for thread in enumerate_threads()
                    if thread.name.startswith("manyfold-delivery-")
                }
                == baseline_threads,
                timeout=1.0,
            )
        )
        self.assertTrue(
            _wait_for(
                lambda: len(os.listdir("/dev/fd")) <= baseline_fds,
                timeout=1.0,
            )
        )

    def _delivery_pair(
        self,
        *,
        retry_initial_seconds: float = 0.05,
    ) -> tuple[DurableDelivery, DurableDelivery]:
        sender_transport, receiver_transport = self._transport_pair()
        return (
            self._track_delivery(
                DurableDelivery(
                    sender_transport,
                    _delivery_config(
                        self._root / "sender.sqlite3",
                        retry_initial_seconds=retry_initial_seconds,
                    ),
                )
            ),
            self._track_delivery(
                DurableDelivery(
                    receiver_transport,
                    _delivery_config(
                        self._root / "receiver.sqlite3",
                        retry_initial_seconds=retry_initial_seconds,
                    ),
                )
            ),
        )

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

    def _track_delivery(self, delivery: DurableDelivery) -> DurableDelivery:
        self._deliveries.append(delivery)
        return delivery

    def _track_transport(self, transport: TcpTransport) -> TcpTransport:
        self._transports.append(transport)
        return transport


def _transport_config(*, max_payload_bytes: int = 65536) -> TransportConfig:
    return TransportConfig(
        security=TransportSecurity.insecure_local_development(),
        outbound_queue_limit=16,
        inbound_queue_limit=16,
        max_payload_bytes=max_payload_bytes,
        connect_timeout=0.1,
        handshake_timeout=0.5,
        heartbeat_interval=0.05,
        peer_timeout=0.5,
        reconnect=ReconnectPolicy(0.02, 1.5, 0.1),
    )


def _delivery_config(
    path: Path,
    *,
    retry_initial_seconds: float = 0.05,
    message_ttl_seconds: float = 5.0,
    dedupe_retention_seconds: float = 5.0,
) -> DeliveryConfig:
    return DeliveryConfig(
        path,
        max_outbox_items=16,
        max_inbox_items=16,
        max_storage_bytes=1024 * 1024,
        receive_queue_limit=4,
        recovery_batch_size=16,
        max_message_bytes=4096,
        message_ttl_seconds=message_ttl_seconds,
        dedupe_retention_seconds=dedupe_retention_seconds,
        retry_initial_seconds=retry_initial_seconds,
        retry_multiplier=1.5,
        retry_max_seconds=0.1,
        topic_policies=_topic_policies(
            ttl_seconds=message_ttl_seconds,
            max_items=16,
            max_bytes=1024 * 1024,
        ),
    )


def _topic_policies(
    *,
    ttl_seconds: float,
    max_items: int,
    max_bytes: int,
) -> tuple[TopicDeliveryPolicy, ...]:
    return tuple(
        TopicDeliveryPolicy.commands(
            topic,
            max_items=max_items,
            max_bytes=max_bytes,
            ttl_seconds=ttl_seconds,
        )
        for topic in (
            "cache.get",
            "events",
            "orders.created",
            "reconnect",
            "recovery",
        )
    )


def _unused_address() -> TcpAddress:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(("127.0.0.1", 0))
        host, port = probe.getsockname()[:2]
        return TcpAddress(str(host), int(port))
    finally:
        probe.close()


def _wait_for(predicate: object, *, timeout: float) -> bool:
    if not callable(predicate):
        raise TypeError("predicate must be callable")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return bool(predicate())


if __name__ == "__main__":
    unittest.main()
