"""Composable PySyncObj network protocols for ManyFold coordinators."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, Protocol, final

from pysyncobj.node import TCPNode
from pysyncobj.transport import TCPTransport, Transport

TransportFactory = Callable[[Any, Any, Iterable[Any]], Transport]

DEFAULT_RAFT_TRANSPORT = "tcp"
DISCONNECT_FAULT_LAYER = "disconnect_faults"
DISCONNECT_MARKER_FILENAME = "network.disconnect"
_SUPPORTED_RAFT_TRANSPORTS = frozenset({DEFAULT_RAFT_TRANSPORT})
_SUPPORTED_TRANSPORT_LAYERS = frozenset({DISCONNECT_FAULT_LAYER})


def resolve_network_protocol(
    config: NetworkProtocolConfig,
) -> RaftNetworkProtocol:
    """Resolve a serializable network configuration to a protocol adapter."""
    if not isinstance(config, NetworkProtocolConfig):
        raise ValueError("network config must be a NetworkProtocolConfig")
    if config.raft_transport == DEFAULT_RAFT_TRANSPORT:
        return TcpRaftNetworkProtocol(config)
    raise ValueError(f"unsupported Raft transport {config.raft_transport!r}")


@final
@dataclass(frozen=True)
class NetworkProtocolConfig:
    """Serializable Raft transport plus ordered composable transport layers."""

    raft_transport: str = DEFAULT_RAFT_TRANSPORT
    layers: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.raft_transport, str):
            raise ValueError("network raft_transport must be a string")
        if self.raft_transport not in _SUPPORTED_RAFT_TRANSPORTS:
            raise ValueError(
                f"unsupported Raft transport {self.raft_transport!r}; "
                f"expected one of {sorted(_SUPPORTED_RAFT_TRANSPORTS)!r}"
            )
        if not isinstance(self.layers, tuple):
            raise ValueError("network protocol layers must be a tuple")
        if not all(isinstance(layer, str) for layer in self.layers):
            raise ValueError("network protocol layers must contain strings")
        if len(set(self.layers)) != len(self.layers):
            raise ValueError("network protocol layers must not contain duplicates")
        unsupported = set(self.layers) - _SUPPORTED_TRANSPORT_LAYERS
        if unsupported:
            raise ValueError(
                f"unsupported network protocol layers {sorted(unsupported)!r}; "
                f"expected values from {sorted(_SUPPORTED_TRANSPORT_LAYERS)!r}"
            )

    @property
    def supports_disconnect_faults(self) -> bool:
        """Return whether the stack exposes marker-controlled disconnects."""
        return DISCONNECT_FAULT_LAYER in self.layers

    def to_dict(self) -> dict[str, object]:
        """Return the stable JSON representation."""
        return {
            "raft_transport": self.raft_transport,
            "layers": list(self.layers),
        }

    @classmethod
    def from_json(cls, value: object) -> NetworkProtocolConfig:
        """Validate a JSON network protocol object."""
        if value is None:
            return cls()
        if not isinstance(value, dict):
            raise ValueError("network protocol config must be a JSON object")
        raft_transport = value.get("raft_transport", DEFAULT_RAFT_TRANSPORT)
        layers = value.get("layers", [])
        if not isinstance(raft_transport, str):
            raise ValueError("network raft_transport must be a string")
        if not isinstance(layers, list) or not all(
            isinstance(layer, str) for layer in layers
        ):
            raise ValueError("network protocol layers must be a list of strings")
        return cls(raft_transport=raft_transport, layers=tuple(layers))


class RaftNetworkProtocol(Protocol):
    """Injectable address/node/transport boundary used by PySyncObj."""

    @property
    def node_class(self) -> type[Any]:
        """Return the PySyncObj node representation class."""
        ...

    def transport_factory(self, state_directory: Path) -> TransportFactory:
        """Build a transport factory, optionally composed with fault layers."""
        ...


@final
class TcpRaftNetworkProtocol:
    """TCP Raft transport with optional ordered wrapper layers."""

    def __init__(self, config: NetworkProtocolConfig) -> None:
        self.config = config

    @property
    def node_class(self) -> type[TCPNode]:
        """Use PySyncObj's address-bound TCP node identity."""
        return TCPNode

    def transport_factory(self, state_directory: Path) -> TransportFactory:
        """Compose the configured transport layers around TCP."""
        factory: TransportFactory = TCPTransport
        for layer in self.config.layers:
            if layer == DISCONNECT_FAULT_LAYER:
                factory = _wrap_disconnect_faults(
                    factory,
                    state_directory / DISCONNECT_MARKER_FILENAME,
                )
                continue
            raise ValueError(f"unsupported network protocol layer {layer!r}")
        return factory


def _wrap_disconnect_faults(
    inner_factory: TransportFactory,
    marker_path: Path,
) -> TransportFactory:
    return partial(
        _DisconnectFaultTransport,
        inner_factory=inner_factory,
        marker_path=marker_path,
    )


@final
class _DisconnectFaultTransport(Transport):
    """Transport decorator that drops all peers while a marker file exists."""

    def __init__(
        self,
        sync_obj: Any,
        self_node: Any,
        other_nodes: Iterable[Any],
        *,
        inner_factory: TransportFactory,
        marker_path: Path,
    ) -> None:
        nodes = tuple(other_nodes)
        super().__init__(sync_obj, self_node, nodes)
        self._sync_obj = sync_obj
        self._marker_path = marker_path
        self._nodes = set(nodes)
        self._is_disconnected = False
        self._inner = inner_factory(sync_obj, self_node, nodes)
        self._inner.setOnMessageReceivedCallback(self._onMessageReceived)
        self._inner.setOnNodeConnectedCallback(self._onNodeConnected)
        self._inner.setOnNodeDisconnectedCallback(self._onNodeDisconnected)
        self._inner.setOnReadonlyNodeConnectedCallback(self._onReadonlyNodeConnected)
        self._inner.setOnReadonlyNodeDisconnectedCallback(
            self._onReadonlyNodeDisconnected
        )
        self._sync_obj.addOnTickCallback(self._synchronize_fault_state)
        self._synchronize_fault_state()

    @property
    def ready(self) -> bool:
        return bool(self._inner.ready)

    def tryGetReady(self) -> None:
        self._inner.tryGetReady()

    def waitReady(self) -> None:
        self._inner.waitReady()

    def setOnUtilityMessageCallback(
        self,
        message: str,
        callback: Callable[..., object] | None,
    ) -> None:
        self._inner.setOnUtilityMessageCallback(message, callback)

    def addNode(self, node: Any) -> None:
        self._nodes.add(node)
        if not self._is_disconnected:
            self._inner.addNode(node)

    def dropNode(self, node: Any) -> None:
        self._nodes.discard(node)
        self._inner.dropNode(node)

    def send(self, node: Any, message: object) -> bool:
        self._synchronize_fault_state()
        if self._is_disconnected:
            return False
        return bool(self._inner.send(node, message))

    def destroy(self) -> None:
        self._sync_obj.removeOnTickCallback(self._synchronize_fault_state)
        self._inner.destroy()
        self._nodes.clear()

    def _synchronize_fault_state(self) -> None:
        should_disconnect = self._marker_path.exists()
        if should_disconnect == self._is_disconnected:
            return
        self._is_disconnected = should_disconnect
        if should_disconnect:
            for node in tuple(self._nodes):
                self._inner.dropNode(node)
                self._onNodeDisconnected(node)
            return
        for node in tuple(self._nodes):
            self._inner.addNode(node)


__all__ = [
    "DEFAULT_RAFT_TRANSPORT",
    "DISCONNECT_FAULT_LAYER",
    "DISCONNECT_MARKER_FILENAME",
    "NetworkProtocolConfig",
    "RaftNetworkProtocol",
    "TcpRaftNetworkProtocol",
    "TransportFactory",
    "resolve_network_protocol",
]
