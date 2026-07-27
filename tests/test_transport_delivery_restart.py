from __future__ import annotations

import socket
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

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
    DeliveryConfig,
    DurableDelivery,
    TopicDeliveryPolicy,
)

from tests.test_support import subprocess_test_env


class TransportDeliveryRestartTests(unittest.TestCase):
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

    def test_public_latest_is_source_keyed_and_requires_source(self) -> None:
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "latest-public"),
                _unused_address(),
                config=_transport_config(),
                expected_peer_node_id="missing",
            )
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                _latest_config(self._root / "latest-public.sqlite3"),
            )
        )
        with self.assertRaisesRegex(ValueError, "requires source"):
            delivery.send(
                TransportMessage(FrameKind.PUBSUB, "state.latest", b"value"),
                message_id="missing-source",
            )
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, "state.latest", b"first"),
            message_id="source-a-1",
            source="source-a",
        )
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, "state.latest", b"second"),
            message_id="source-a-2",
            source="source-a",
        )
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, "state.latest", b"other"),
            message_id="source-b-1",
            source="source-b",
        )

        health = delivery.health()
        self.assertEqual(health.outbox_items, 2)
        self.assertEqual(health.latest_outbox_items, 2)
        self.assertEqual(health.coalesced, 1)

    def test_latest_hard_exit_recovers_only_newest_value_per_source(
        self,
    ) -> None:
        address = _unused_address()
        sender_path = self._root / "latest-crashed.sqlite3"
        script = """
import os
import sys
from pathlib import Path
from manyfold.architecture.transport import (
    FrameKind, NodeIdentity, ReconnectPolicy, TcpAddress, TcpTransport,
    TransportConfig, TransportMessage, TransportSecurity,
)
from manyfold.architecture.transport_delivery import (
    DeliveryConfig, DurableDelivery, TopicDeliveryPolicy,
)

transport = TcpTransport.connect(
    NodeIdentity("cluster", "sender", "latest-crashed"),
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
policy = TopicDeliveryPolicy.latest(
    "state.latest",
    max_sources=4,
    max_bytes=65536,
    ttl_seconds=5.0,
    max_inbox_items=4,
)
delivery = DurableDelivery(
    transport,
    DeliveryConfig(
        Path(sys.argv[3]),
        max_outbox_items=4,
        max_inbox_items=4,
        max_storage_bytes=1024 * 1024,
        recovery_batch_size=4,
        max_message_bytes=4096,
        message_ttl_seconds=5.0,
        retry_initial_seconds=0.02,
        retry_max_seconds=0.1,
        topic_policies=(policy,),
    ),
)
delivery.send(
    TransportMessage(FrameKind.PUBSUB, "state.latest", b"old-a"),
    message_id="a-old",
    source="source-a",
)
delivery.send(
    TransportMessage(FrameKind.PUBSUB, "state.latest", b"new-a"),
    message_id="a-new",
    source="source-a",
)
delivery.send(
    TransportMessage(FrameKind.PUBSUB, "state.latest", b"value-b"),
    message_id="b-value",
    source="source-b",
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
                str(sender_path),
            ],
            check=False,
            env=subprocess_test_env(),
            timeout=5.0,
        )
        self.assertEqual(completed.returncode, 0)
        server = self._track_transport(
            TcpTransport.listen(
                NodeIdentity("cluster", "receiver", "receiver-latest"),
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
                _latest_config(self._root / "latest-receiver.sqlite3"),
            )
        )
        sender = self._track_delivery(
            DurableDelivery(client, _latest_config(sender_path))
        )
        received = [receiver.receive(timeout=3.0) for _ in range(2)]
        for item in received:
            receiver.ack(item.message_id)

        self.assertEqual(
            {item.message_id: item.message.payload for item in received},
            {"a-new": b"new-a", "b-value": b"value-b"},
        )
        self.assertTrue(sender.flush(timeout=2.0))
        self.assertEqual(sender.health().coalesced, 0)

    def _track_delivery(self, delivery: DurableDelivery) -> DurableDelivery:
        self._deliveries.append(delivery)
        return delivery

    def _track_transport(self, transport: TcpTransport) -> TcpTransport:
        self._transports.append(transport)
        return transport


def _latest_config(path: Path) -> DeliveryConfig:
    policy = TopicDeliveryPolicy.latest(
        "state.latest",
        max_sources=4,
        max_bytes=65536,
        ttl_seconds=5.0,
        max_inbox_items=4,
    )
    return DeliveryConfig(
        path,
        max_outbox_items=4,
        max_inbox_items=4,
        max_storage_bytes=1024 * 1024,
        recovery_batch_size=4,
        max_message_bytes=4096,
        message_ttl_seconds=5.0,
        retry_initial_seconds=0.02,
        retry_max_seconds=0.1,
        topic_policies=(policy,),
    )


def _transport_config() -> TransportConfig:
    return TransportConfig(
        security=TransportSecurity.insecure_local_development(),
        outbound_queue_limit=16,
        inbound_queue_limit=16,
        max_payload_bytes=65536,
        connect_timeout=0.1,
        handshake_timeout=0.5,
        heartbeat_interval=0.05,
        peer_timeout=0.5,
        reconnect=ReconnectPolicy(0.02, 1.5, 0.1),
    )


def _unused_address() -> TcpAddress:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(("127.0.0.1", 0))
        host, port = probe.getsockname()[:2]
        return TcpAddress(str(host), int(port))
    finally:
        probe.close()
