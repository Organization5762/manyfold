from __future__ import annotations

import socket
import time
import unittest
from threading import Event

from manyfold.architecture import _transport_rpc_codec as _rpc_codec
from manyfold.architecture.transport import (
    NodeIdentity,
    ReconnectPolicy,
    TcpAddress,
    TcpTransport,
    TransportConfig,
    TransportSecurity,
)
from manyfold.architecture.transport_rpc import (
    RpcCancelled,
    RpcConfig,
    RpcDisconnected,
    RpcEndpoint,
    RpcEndpointClosed,
    RpcOverloaded,
    RpcProtocolError,
    RpcRemoteError,
    RpcRequest,
    RpcShutdownTimeout,
    RpcTimeout,
)


class ArchitectureTransportRpcTests(unittest.TestCase):
    def setUp(self) -> None:
        self._endpoints: list[RpcEndpoint] = []
        self._transports: list[TcpTransport] = []

    def tearDown(self) -> None:
        for endpoint in reversed(self._endpoints):
            endpoint.close()
        for transport in reversed(self._transports):
            transport.close()

    def test_real_transport_calls_handler_and_preserves_remote_errors(self) -> None:
        server, client = self._rpc_pair()
        server.register(
            "coordinator",
            "allocate",
            lambda request, cancellation: request.payload.upper(),
        )

        response = client.call(
            "coordinator",
            "allocate",
            b"worker-7",
            correlation_id="allocate-1",
        )
        server.register(
            "coordinator",
            "fail",
            lambda request, cancellation: (_ for _ in ()).throw(
                RuntimeError("capacity database unavailable")
            ),
        )

        self.assertEqual(response, b"WORKER-7")
        with self.assertRaises(RpcRemoteError) as raised:
            client.call("coordinator", "fail", b"")
        self.assertEqual(raised.exception.code, "handler_error")
        self.assertEqual(
            raised.exception.remote_message,
            "remote RPC handler failed",
        )
        self.assertFalse(raised.exception.retryable)
        self.assertEqual(
            server.health().last_error,
            "RuntimeError: capacity database unavailable",
        )
        health = client.health()
        self.assertEqual(health.calls_completed, 1)
        self.assertEqual(health.calls_failed, 1)

    def test_deadline_propagates_cancellation_to_active_handler(self) -> None:
        server, client = self._rpc_pair()
        entered = Event()
        cancelled = Event()

        def wait_for_cancel(request: object, cancellation: object) -> bytes:
            entered.set()
            if cancellation.wait(timeout=1.0):
                cancelled.set()
            cancellation.raise_if_cancelled()
            return b"too-late"

        server.register("jobs", "wait", wait_for_cancel)

        with self.assertRaises(RpcTimeout):
            client.call(
                "jobs",
                "wait",
                b"",
                timeout_seconds=0.1,
                correlation_id="timeout-1",
            )

        self.assertTrue(entered.is_set())
        self.assertTrue(cancelled.wait(timeout=1.0))
        self.assertEqual(client.health().calls_timed_out, 1)
        self.assertTrue(
            _wait_for(
                lambda: server.health().active_requests == 0,
                timeout=1.0,
            )
        )

    def test_explicit_call_cancellation_reaches_remote_context(self) -> None:
        server, client = self._rpc_pair()
        entered = Event()
        cancelled = Event()

        def cancellable(request: object, cancellation: object) -> bytes:
            entered.set()
            cancellation.wait(timeout=2.0)
            if cancellation.is_cancelled:
                cancelled.set()
            cancellation.raise_if_cancelled()
            return b"done"

        server.register("jobs", "cancel", cancellable)
        call = client.start_call(
            "jobs",
            "cancel",
            b"",
            timeout_seconds=2.0,
            correlation_id="cancel-1",
        )
        self.assertTrue(entered.wait(timeout=1.0))

        self.assertTrue(call.cancel("coordinator reassigned work"))
        self.assertFalse(call.cancel())
        with self.assertRaises(RpcCancelled):
            call.result()

        self.assertTrue(cancelled.wait(timeout=1.0))
        self.assertEqual(client.health().calls_cancelled, 1)

    def test_server_queue_and_client_in_flight_limits_apply_backpressure(self) -> None:
        server, client = self._rpc_pair(
            server_config=RpcConfig(
                max_in_flight=4,
                max_workers=1,
                request_queue_limit=1,
                receive_poll_seconds=0.01,
            ),
            client_config=RpcConfig(
                max_in_flight=3,
                receive_poll_seconds=0.01,
            ),
        )
        entered = Event()

        def blocked(request: object, cancellation: object) -> bytes:
            entered.set()
            cancellation.wait(timeout=2.0)
            cancellation.raise_if_cancelled()
            return b"done"

        server.register("jobs", "blocked", blocked)
        first = client.start_call(
            "jobs",
            "blocked",
            b"first",
            timeout_seconds=2.0,
        )
        self.assertTrue(entered.wait(timeout=1.0))
        second = client.start_call(
            "jobs",
            "blocked",
            b"second",
            timeout_seconds=2.0,
        )
        self.assertTrue(
            _wait_for(
                lambda: server.health().queued_requests == 1,
                timeout=1.0,
            )
        )
        third = client.start_call(
            "jobs",
            "blocked",
            b"third",
            timeout_seconds=2.0,
        )

        with self.assertRaises(RpcRemoteError) as raised:
            third.result()
        self.assertEqual(raised.exception.code, "overloaded")
        self.assertTrue(raised.exception.retryable)
        self.assertEqual(server.health().requests_overloaded, 1)
        self.assertTrue(first.cancel())
        self.assertTrue(second.cancel())

    def test_client_in_flight_limit_rejects_before_retaining_payload(self) -> None:
        server, client = self._rpc_pair(
            client_config=RpcConfig(max_in_flight=1),
        )
        entered = Event()

        def blocked(request: object, cancellation: object) -> bytes:
            entered.set()
            cancellation.wait(timeout=2.0)
            cancellation.raise_if_cancelled()
            return b"done"

        server.register("jobs", "blocked", blocked)
        first = client.start_call(
            "jobs",
            "blocked",
            b"retained",
            timeout_seconds=2.0,
        )
        self.assertTrue(entered.wait(timeout=1.0))

        with self.assertRaisesRegex(RpcOverloaded, "max_in_flight"):
            client.start_call("jobs", "blocked", b"rejected")

        self.assertEqual(client.health().pending_calls, 1)
        self.assertTrue(first.cancel())

    def test_disconnected_endpoint_rejects_before_retaining_payload(self) -> None:
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "unused", "unused-1"),
                _unused_address(),
                config=_transport_config(),
            )
        )
        endpoint = self._track_endpoint(RpcEndpoint(transport))

        with self.assertRaises(RpcDisconnected):
            endpoint.start_call("service", "method", b"payload")
        self.assertEqual(endpoint.health().pending_calls, 0)

    def test_handler_registry_is_bounded(self) -> None:
        server, _ = self._rpc_pair(
            server_config=RpcConfig(max_handlers=1),
        )
        server.register("service", "first", lambda request, cancellation: b"ok")

        with self.assertRaisesRegex(RpcOverloaded, "max_handlers"):
            server.register(
                "service",
                "second",
                lambda request, cancellation: b"not accepted",
            )

        self.assertTrue(server.unregister("service", "first"))
        self.assertFalse(server.unregister("service", "first"))
        server.register("service", "second", lambda request, cancellation: b"ok")

    def test_configuration_rejects_non_finite_deadlines(self) -> None:
        with self.assertRaisesRegex(ValueError, "positive number"):
            RpcConfig(default_timeout_seconds=float("inf"))
        with self.assertRaisesRegex(ValueError, "non-negative number"):
            RpcConfig(send_timeout_seconds=float("nan"))

    def test_abandoned_call_expires_and_releases_its_slot(self) -> None:
        server, client = self._rpc_pair(
            client_config=RpcConfig(
                max_in_flight=1,
                receive_poll_seconds=0.01,
            )
        )
        cancelled = Event()

        def wait_for_deadline(request: object, cancellation: object) -> bytes:
            cancellation.wait(timeout=1.0)
            if cancellation.is_cancelled:
                cancelled.set()
            cancellation.raise_if_cancelled()
            return b"late"

        server.register("jobs", "abandoned", wait_for_deadline)
        call = client.start_call(
            "jobs",
            "abandoned",
            b"retained-until-deadline",
            timeout_seconds=0.05,
        )

        self.assertTrue(
            _wait_for(lambda: client.health().pending_calls == 0, timeout=1.0)
        )
        self.assertTrue(call.is_done)
        with self.assertRaises(RpcTimeout):
            call.result()
        self.assertTrue(cancelled.wait(timeout=1.0))
        self.assertEqual(client.health().calls_timed_out, 1)

    def test_framing_failures_release_calls_and_preserve_worker_capacity(
        self,
    ) -> None:
        server, client = self._rpc_pair()
        server.register(
            "payloads",
            "echo",
            lambda request, cancellation: request.payload,
        )
        server.register(
            "payloads",
            "oversized",
            lambda request, cancellation: b"x" * 5000,
        )

        with self.assertRaisesRegex(RpcProtocolError, "cannot be framed"):
            client.start_call("payloads", "echo", b"x" * 5000)
        self.assertEqual(client.health().pending_calls, 0)
        with self.assertRaises(RpcRemoteError) as raised:
            client.call("payloads", "oversized", b"")
        self.assertEqual(raised.exception.code, "response_too_large")
        self.assertEqual(server.health().workers_alive, server.config.max_workers)
        self.assertEqual(client.call("payloads", "echo", b"ok"), b"ok")

    def test_repeated_calls_release_all_bounded_correlation_state(self) -> None:
        server, client = self._rpc_pair(
            server_config=RpcConfig(
                max_workers=2,
                request_queue_limit=2,
                receive_poll_seconds=0.01,
            ),
            client_config=RpcConfig(
                max_in_flight=2,
                receive_poll_seconds=0.01,
            ),
        )
        server.register(
            "coordinator",
            "ping",
            lambda request, cancellation: request.payload,
        )

        for sequence in range(64):
            payload = sequence.to_bytes(2, "big")
            self.assertEqual(
                client.call("coordinator", "ping", payload),
                payload,
            )

        self.assertEqual(client.health().pending_calls, 0)
        self.assertEqual(server.health().queued_requests, 0)
        self.assertEqual(server.health().active_requests, 0)
        self.assertEqual(client.health().calls_completed, 64)

    def test_disconnect_fails_pending_call_and_reconnect_accepts_new_calls(
        self,
    ) -> None:
        address = _unused_address()
        transport_config = _transport_config()
        server_identity = NodeIdentity("cluster", "server", "server-1")
        server_transport = self._track_transport(
            TcpTransport.listen(
                server_identity,
                address,
                config=transport_config,
                expected_peer_node_id="client",
            )
        )
        client_transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "client", "client-1"),
                address,
                config=transport_config,
                expected_peer_node_id="server",
            )
        )
        self.assertTrue(server_transport.wait_until_connected(timeout=2.0))
        self.assertTrue(client_transport.wait_until_connected(timeout=2.0))
        server = self._track_endpoint(
            RpcEndpoint(
                server_transport,
                config=RpcConfig(receive_poll_seconds=0.01),
            )
        )
        client = self._track_endpoint(
            RpcEndpoint(
                client_transport,
                config=RpcConfig(receive_poll_seconds=0.01),
            )
        )
        entered = Event()

        def interrupted(request: object, cancellation: object) -> bytes:
            entered.set()
            cancellation.wait(timeout=2.0)
            cancellation.raise_if_cancelled()
            return b"stale"

        server.register("jobs", "run", interrupted)
        pending = client.start_call(
            "jobs",
            "run",
            b"",
            timeout_seconds=2.0,
        )
        self.assertTrue(entered.wait(timeout=1.0))

        server_transport.close()

        with self.assertRaises(RpcDisconnected):
            pending.result()
        server.close()
        replacement_transport = self._track_transport(
            TcpTransport.listen(
                NodeIdentity("cluster", "server", "server-2"),
                address,
                config=transport_config,
                expected_peer_node_id="client",
            )
        )
        self.assertTrue(client_transport.wait_until_connected(timeout=2.0))
        self.assertTrue(replacement_transport.wait_until_connected(timeout=2.0))
        replacement = self._track_endpoint(
            RpcEndpoint(
                replacement_transport,
                config=RpcConfig(receive_poll_seconds=0.01),
            )
        )
        stale_request_executed = Event()

        def replacement_handler(request: object, cancellation: object) -> bytes:
            if request.payload == b"stale":
                stale_request_executed.set()
            return b"fresh"

        replacement.register(
            "jobs",
            "run",
            replacement_handler,
        )
        self.assertTrue(client.wait_until_ready(timeout=2.0))
        self.assertTrue(replacement.wait_until_ready(timeout=2.0))
        client_transport.send(
            _rpc_codec.encode(
                RpcRequest(
                    correlation_id="stale-session-request",
                    service="jobs",
                    method="run",
                    payload=b"stale",
                    timeout_seconds=1.0,
                    session_id="client-1:1",
                )
            )
        )
        time.sleep(0.1)
        self.assertFalse(stale_request_executed.is_set())

        self.assertEqual(
            client.call("jobs", "run", b"", timeout_seconds=1.0),
            b"fresh",
        )

    def test_disposal_cancels_calls_and_joins_owned_workers(self) -> None:
        server, client = self._rpc_pair()
        entered = Event()
        cancelled = Event()

        def active(request: object, cancellation: object) -> bytes:
            entered.set()
            cancellation.wait(timeout=2.0)
            if cancellation.is_cancelled:
                cancelled.set()
            cancellation.raise_if_cancelled()
            return b"late"

        server.register("jobs", "active", active)
        call = client.start_call(
            "jobs",
            "active",
            b"",
            timeout_seconds=2.0,
        )
        self.assertTrue(entered.wait(timeout=1.0))

        client.close()

        with self.assertRaises(RpcEndpointClosed):
            call.result()
        self.assertTrue(cancelled.wait(timeout=1.0))
        self.assertFalse(client._receiver.is_alive())
        self.assertFalse(any(worker.is_alive() for worker in client._workers))
        self.assertEqual(client.health().pending_calls, 0)
        server.close()
        self.assertEqual(server.health().handlers, 0)
        self.assertFalse(server._receiver.is_alive())
        self.assertFalse(any(worker.is_alive() for worker in server._workers))

    def test_shutdown_timeout_can_be_retried_after_handler_exits(self) -> None:
        server, client = self._rpc_pair(
            server_config=RpcConfig(
                receive_poll_seconds=0.01,
                shutdown_timeout_seconds=0.05,
            )
        )
        entered = Event()
        release = Event()

        def ignores_cancellation(request: object, cancellation: object) -> bytes:
            entered.set()
            release.wait(timeout=1.0)
            return b"done"

        server.register("jobs", "ignore", ignores_cancellation)
        call = client.start_call("jobs", "ignore", b"", timeout_seconds=1.0)
        self.assertTrue(entered.wait(timeout=1.0))

        with self.assertRaises(RpcShutdownTimeout):
            server.close()
        release.set()
        server.close()
        self.assertFalse(any(worker.is_alive() for worker in server._workers))
        self.assertTrue(call.cancel())

    def _rpc_pair(
        self,
        *,
        server_config: RpcConfig | None = None,
        client_config: RpcConfig | None = None,
    ) -> tuple[RpcEndpoint, RpcEndpoint]:
        server_transport = self._track_transport(
            TcpTransport.listen(
                NodeIdentity("cluster", "server", "server-1"),
                config=_transport_config(),
                expected_peer_node_id="client",
            )
        )
        client_transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "client", "client-1"),
                server_transport.address,
                config=_transport_config(),
                expected_peer_node_id="server",
            )
        )
        self.assertTrue(server_transport.wait_until_connected(timeout=2.0))
        self.assertTrue(client_transport.wait_until_connected(timeout=2.0))
        server = self._track_endpoint(
            RpcEndpoint(server_transport, config=server_config)
        )
        client = self._track_endpoint(
            RpcEndpoint(client_transport, config=client_config)
        )
        self.assertTrue(server.wait_until_ready(timeout=2.0))
        self.assertTrue(client.wait_until_ready(timeout=2.0))
        return server, client

    def _track_endpoint(self, endpoint: RpcEndpoint) -> RpcEndpoint:
        self._endpoints.append(endpoint)
        return endpoint

    def _track_transport(self, transport: TcpTransport) -> TcpTransport:
        self._transports.append(transport)
        return transport


def _transport_config() -> TransportConfig:
    return TransportConfig(
        security=TransportSecurity.insecure_local_development(),
        outbound_queue_limit=32,
        inbound_queue_limit=32,
        max_payload_bytes=4096,
        connect_timeout=0.1,
        handshake_timeout=0.5,
        heartbeat_interval=0.05,
        peer_timeout=0.5,
        reconnect=ReconnectPolicy(
            initial_delay=0.02,
            multiplier=1.5,
            max_delay=0.1,
        ),
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
