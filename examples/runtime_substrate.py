"""Small runtime substrate example for shifting dormant processes into roles."""

from __future__ import annotations

import argparse
import json
import os
import socket
import socketserver
import struct
import tempfile
import threading
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from time import perf_counter, sleep
from typing import Any, Literal, TypedDict

from manyfold import Graph, Schema, route

ShiftAction = Literal["shift", "retain", "move"]
SlotState = Literal["dormant", "active"]

_DEFAULT_DORMANT_PROCESS_COUNT = 10
_DEFAULT_CONTROLLER_DORMANT_BUFFER = 2
_DEFAULT_CONTROLLER_MAX_PROCESSES = 32
_DEFAULT_CONTROLLER_SPAWN_BATCH_SIZE = 2


class KafkaBenchmarkMode(StrEnum):
    Memory = "memory"
    Graph = "graph"
    Disk = "disk"
    Socket = "socket"
    Full = "full"


@dataclass(frozen=True, slots=True)
class ResourceObservation:
    """Current pressure observed by one computer runtime."""

    cpu: float = 0.0
    memory: float = 0.0

    def __post_init__(self) -> None:
        _require_ratio(self.cpu, "cpu")
        _require_ratio(self.memory, "memory")

    @property
    def pressure(self) -> float:
        return max(self.cpu, self.memory)


@dataclass(frozen=True, slots=True)
class ShiftSpec:
    """Desired role for a dormant ManyFold process to become."""

    name: str
    capability: str
    data_dir: Path

    def __post_init__(self) -> None:
        _require_non_empty_string(self.name, "shift name")
        _require_non_empty_string(self.capability, "shift capability")
        if not isinstance(self.data_dir, Path):
            object.__setattr__(self, "data_dir", Path(self.data_dir))


@dataclass(frozen=True, slots=True)
class FrozenProcess:
    """Warm process owned by a computer runtime, waiting to be shifted."""

    runtime: str
    slot: int
    address: str
    state: SlotState

    def __post_init__(self) -> None:
        _require_non_empty_string(self.runtime, "frozen process runtime")
        _require_positive_int(self.slot, "frozen process slot")
        _require_non_empty_string(self.address, "frozen process address")
        if self.state not in ("dormant", "active"):
            raise ValueError("frozen process state must be dormant or active")


@dataclass(frozen=True, slots=True)
class ProcessSnapshot:
    """Transferable process state captured before moving work."""

    process: str
    capability: str
    source_runtime: str
    source_address: str
    generation: int

    def __post_init__(self) -> None:
        _require_non_empty_string(self.process, "snapshot process")
        _require_non_empty_string(self.capability, "snapshot capability")
        _require_non_empty_string(self.source_runtime, "snapshot source runtime")
        _require_non_empty_string(self.source_address, "snapshot source address")
        _require_positive_int(self.generation, "snapshot generation")


@dataclass(frozen=True, slots=True)
class ManyFoldControllerPolicy:
    """Bounds for controller-managed dormant ManyFold process capacity."""

    dormant_buffer: int = _DEFAULT_CONTROLLER_DORMANT_BUFFER
    max_processes: int = _DEFAULT_CONTROLLER_MAX_PROCESSES
    spawn_batch_size: int = _DEFAULT_CONTROLLER_SPAWN_BATCH_SIZE

    def __post_init__(self) -> None:
        _require_non_negative_int(self.dormant_buffer, "controller dormant_buffer")
        _require_positive_int(
            self.max_processes,
            "controller max_processes",
        )
        _require_positive_int(self.spawn_batch_size, "controller spawn_batch_size")
        if self.dormant_buffer > self.max_processes:
            raise ValueError("controller dormant_buffer cannot exceed max_processes")


@dataclass(frozen=True, slots=True)
class ManyFoldControllerSnapshot:
    """Current controller view of instances, active roles, and spare capacity."""

    controller: str
    runtimes: tuple[str, ...]
    active_processes: int
    dormant_processes: int
    dormant_buffer: int
    max_processes: int

    def __post_init__(self) -> None:
        _require_non_empty_string(self.controller, "controller name")
        if not isinstance(self.runtimes, tuple) or not self.runtimes:
            raise ValueError("controller runtimes must be a non-empty tuple")
        for runtime in self.runtimes:
            _require_non_empty_string(runtime, "controller runtime")
        _require_non_negative_int(self.active_processes, "controller active_processes")
        _require_non_negative_int(self.dormant_processes, "controller dormant_processes")
        _require_non_negative_int(self.dormant_buffer, "controller dormant_buffer")
        _require_positive_int(
            self.max_processes,
            "controller max_processes",
        )


@dataclass(frozen=True, slots=True)
class ProcessHandle:
    """An active role currently occupied by one shifted ManyFold process."""

    runtime: str
    slot: int
    name: str
    capability: str
    address: str
    data_dir: Path
    generation: int
    proxy_from: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_non_empty_string(self.runtime, "process runtime")
        _require_positive_int(self.slot, "process slot")
        _require_non_empty_string(self.name, "process name")
        _require_non_empty_string(self.capability, "process capability")
        _require_non_empty_string(self.address, "process address")
        _require_positive_int(self.generation, "process generation")
        if not isinstance(self.data_dir, Path):
            object.__setattr__(self, "data_dir", Path(self.data_dir))
        if not isinstance(self.proxy_from, tuple):
            raise TypeError("process proxy_from must be a tuple")
        for address in self.proxy_from:
            _require_non_empty_string(address, "process proxy address")


@dataclass(frozen=True, slots=True)
class ShiftResult:
    """Result of shifting, retaining, or moving a process role."""

    spec: ShiftSpec
    handle: ProcessHandle
    action: ShiftAction
    snapshot: ProcessSnapshot | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.spec, ShiftSpec):
            raise TypeError("shift result spec must be a ShiftSpec")
        if not isinstance(self.handle, ProcessHandle):
            raise TypeError("shift result handle must be a ProcessHandle")
        if self.action not in ("shift", "retain", "move"):
            raise ValueError("shift result action must be shift, retain, or move")
        if self.snapshot is not None and not isinstance(self.snapshot, ProcessSnapshot):
            raise TypeError("shift result snapshot must be a ProcessSnapshot")


class ManyFoldStream:
    """Named local stream endpoint used to compose runtime IO blocks."""

    def __init__(
        self,
        *,
        owner: str,
        family: str,
        stream: str,
        schema_name: str,
    ) -> None:
        _require_non_empty_string(owner, "stream owner")
        _require_non_empty_string(family, "stream family")
        _require_non_empty_string(stream, "stream name")
        _require_non_empty_string(schema_name, "stream schema name")
        self.graph = Graph()
        self.route = route(
            owner=owner,
            family=family,
            stream=stream,
            schema=Schema.any(schema_name),
        )

    @property
    def name(self) -> str:
        return self.route.display()

    def publish(self, value: Any) -> None:
        self.graph.publish(self.route, value)

    def latest(self) -> Any:
        latest = self.graph.latest(self.route)
        return None if latest is None else latest.value


class InputPort:
    """Typed stream input that can receive values from any connected output."""

    def __init__(
        self,
        *,
        owner: str,
        name: str,
        handler: Callable[[Any], Any],
    ) -> None:
        _require_non_empty_string(owner, "input port owner")
        _require_non_empty_string(name, "input port name")
        if not callable(handler):
            raise TypeError("input port handler must be callable")
        self.owner = owner
        self.name = name
        self.stream = ManyFoldStream(
            owner=owner,
            family="input",
            stream=name,
            schema_name="InputPortEvent",
        )
        self._handler = handler

    def receive(self, value: Any) -> Any:
        self.stream.publish(value)
        return self._handler(value)


class OutputPort:
    """Typed stream output that can connect to one or more inputs."""

    def __init__(self, *, owner: str, name: str) -> None:
        _require_non_empty_string(owner, "output port owner")
        _require_non_empty_string(name, "output port name")
        self.owner = owner
        self.name = name
        self.stream = ManyFoldStream(
            owner=owner,
            family="output",
            stream=name,
            schema_name="OutputPortEvent",
        )
        self._connections: list[StreamConnection] = []

    def connect(
        self,
        target: InputPort,
        *,
        transport: "StreamTransport | None" = None,
    ) -> "StreamConnection":
        if not isinstance(target, InputPort):
            raise TypeError("output port target must be an InputPort")
        resolved_transport = DirectStreamTransport() if transport is None else transport
        if not isinstance(resolved_transport, StreamTransport):
            raise TypeError("output port transport must be a StreamTransport")
        connection = StreamConnection(
            source=self,
            target=target,
            transport=resolved_transport,
        )
        self._connections.append(connection)
        return connection

    def emit(self, value: Any) -> tuple[Any, ...]:
        self.stream.publish(value)
        return tuple(connection.send(value) for connection in self._connections)


@dataclass(frozen=True, slots=True)
class StreamConnection:
    source: OutputPort
    target: InputPort
    transport: "StreamTransport"

    def __post_init__(self) -> None:
        if not isinstance(self.source, OutputPort):
            raise TypeError("stream connection source must be an OutputPort")
        if not isinstance(self.target, InputPort):
            raise TypeError("stream connection target must be an InputPort")
        if not isinstance(self.transport, StreamTransport):
            raise TypeError("stream connection transport must be a StreamTransport")

    def send(self, value: Any) -> Any:
        return self.transport.deliver(self.target, value)


class StreamTransport:
    """Transport implementation for one output->input stream edge."""

    name = "base"

    def deliver(self, target: InputPort, value: Any) -> Any:
        raise NotImplementedError


class DirectStreamTransport(StreamTransport):
    name = "direct"

    def deliver(self, target: InputPort, value: Any) -> Any:
        return target.receive(value)


class TcpLoopbackStreamTransport(StreamTransport):
    """TCP implementation for a bytes-in/bytes-out stream edge."""

    name = "tcp_loopback"

    def deliver(self, target: InputPort, value: Any) -> Any:
        if not isinstance(value, bytes):
            raise TypeError("TCP stream transport only supports bytes payloads")

        def handle(payload: bytes) -> bytes:
            response = target.receive(payload)
            if not isinstance(response, bytes):
                raise TypeError("TCP stream target must return bytes")
            return response

        with TcpStreamServer(name=f"{target.owner}-{target.name}", handler=handle) as server:
            return server.client().request(value)


@dataclass(frozen=True, slots=True)
class KafkaInputs:
    event_stream: InputPort


@dataclass(frozen=True, slots=True)
class KafkaOutputs:
    records: OutputPort


@dataclass(frozen=True, slots=True)
class LoadBalancerInputs:
    application: InputPort


@dataclass(frozen=True, slots=True)
class LoadBalancerOutputs:
    application: OutputPort


@dataclass(frozen=True, slots=True)
class KafkaProducerOutputs:
    produce: OutputPort


@dataclass(frozen=True, slots=True)
class KafkaCoordination:
    service: str
    endpoint: str
    edges: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        _require_non_empty_string(self.service, "Kafka coordination service")
        _require_non_empty_string(self.endpoint, "Kafka coordination endpoint")
        if not isinstance(self.edges, tuple) or not self.edges:
            raise ValueError("Kafka coordination edges must be a non-empty tuple")
        for source, target in self.edges:
            _require_non_empty_string(source, "Kafka coordination edge source")
            _require_non_empty_string(target, "Kafka coordination edge target")


@dataclass(frozen=True, slots=True)
class KafkaConsumerInputs:
    records: InputPort


@dataclass(frozen=True, slots=True)
class KafkaProduceInputs:
    tcp_request: InputPort


@dataclass(frozen=True, slots=True)
class TopicSubscriberInputs:
    messages: InputPort


@dataclass(frozen=True, slots=True)
class KafkaRecord:
    offset: int
    payload: bytes
    leader: str

    def __post_init__(self) -> None:
        _require_non_negative_int(self.offset, "Kafka record offset")
        if not isinstance(self.payload, bytes) or not self.payload:
            raise ValueError("Kafka record payload must be non-empty bytes")
        _require_non_empty_string(self.leader, "Kafka record leader")


@dataclass(frozen=True, slots=True)
class NetworkTopologyConfiguration:
    service: str
    endpoint: str
    edges: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        _require_non_empty_string(self.service, "network topology service")
        _require_non_empty_string(self.endpoint, "network topology endpoint")
        if not isinstance(self.edges, tuple) or not self.edges:
            raise ValueError("network topology edges must be a non-empty tuple")
        for source, target in self.edges:
            _require_non_empty_string(source, "network topology edge source")
            _require_non_empty_string(target, "network topology edge target")


@dataclass(frozen=True, slots=True)
class PeerLivenessSnapshot:
    nodes: tuple[str, ...]
    live_nodes: tuple[str, ...]
    failed_nodes: tuple[str, ...]
    is_degraded: bool

    def __post_init__(self) -> None:
        nodes = set(_require_node_set(self.nodes, "peer liveness nodes"))
        live_nodes = set(_require_node_set(self.live_nodes, "peer liveness live_nodes"))
        failed_nodes = set(self.failed_nodes)
        for node in self.failed_nodes:
            _require_non_empty_string(node, "peer liveness failed_nodes")
        if not live_nodes.issubset(nodes):
            raise ValueError("peer liveness live_nodes must be declared nodes")
        if not failed_nodes.issubset(nodes):
            raise ValueError("peer liveness failed_nodes must be declared nodes")
        if live_nodes & failed_nodes:
            raise ValueError("peer liveness live and failed nodes must not overlap")
        if not isinstance(self.is_degraded, bool):
            raise TypeError("peer liveness is_degraded must be a boolean")


@dataclass(frozen=True, slots=True)
class PeerLivenessRoutes:
    membership: object
    heartbeat: object
    node_failed: object
    liveness_state: object


class PeerLivenessSubgraph:
    """Reusable ManyFold subgraph for peer heartbeat and failure state."""

    def __init__(
        self,
        *,
        graph: Graph,
        routes: PeerLivenessRoutes,
        service: str,
        nodes: tuple[str, ...],
    ) -> None:
        _require_non_empty_string(service, "peer liveness service")
        self.graph = graph
        self.routes = routes
        self.service = service
        self.nodes = _require_node_set(nodes, "peer liveness nodes")
        self._live_nodes = set(self.nodes)
        self._publish_membership()
        self._publish_snapshot()

    @classmethod
    def install(
        cls,
        *,
        service: str,
        nodes: tuple[str, ...],
    ) -> "PeerLivenessSubgraph":
        graph = Graph()
        routes = PeerLivenessRoutes(
            membership=route(
                owner=f"{service}_peer_liveness",
                family="membership",
                stream="members",
                schema=Schema.any("PeerLivenessMembership"),
            ),
            heartbeat=route(
                owner=f"{service}_peer_liveness",
                family="heartbeat",
                stream="seen",
                schema=Schema.any("PeerHeartbeat"),
            ),
            node_failed=route(
                owner=f"{service}_peer_liveness",
                family="heartbeat",
                stream="node_failed",
                schema=Schema.any("PeerNodeFailed"),
            ),
            liveness_state=route(
                owner=f"{service}_peer_liveness",
                family="heartbeat",
                stream="liveness_state",
                schema=Schema.any("PeerLivenessSnapshot"),
            ),
        )
        graph.register_diagram_node(
            f"{service}-peer-liveness",
            input_routes=(routes.membership, routes.heartbeat, routes.node_failed),
            output_routes=(routes.liveness_state,),
            group="peer liveness",
        )
        return cls(graph=graph, routes=routes, service=service, nodes=nodes)

    @property
    def live_nodes(self) -> tuple[str, ...]:
        return tuple(sorted(self._live_nodes))

    def heartbeat(self, node: str) -> PeerLivenessSnapshot:
        _require_non_empty_string(node, "peer liveness heartbeat node")
        if node not in self.nodes:
            raise RuntimeError(f"peer liveness node is not declared: {node}")
        self.graph.publish(self.routes.heartbeat, node)
        if node not in self._live_nodes:
            self._live_nodes.add(node)
        return self._publish_snapshot()

    def mark_failed(self, node: str) -> PeerLivenessSnapshot:
        _require_non_empty_string(node, "peer liveness failed node")
        if node not in self._live_nodes:
            raise RuntimeError(f"peer liveness node is not live: {node}")
        self._live_nodes.remove(node)
        self.graph.publish(self.routes.node_failed, node)
        return self._publish_snapshot()

    def snapshot(self) -> PeerLivenessSnapshot:
        latest = self.graph.latest(self.routes.liveness_state)
        if latest is None:
            raise RuntimeError("peer liveness has not published state")
        return latest.value

    def _publish_membership(self) -> None:
        self.graph.publish(self.routes.membership, self.nodes)

    def _publish_snapshot(self) -> PeerLivenessSnapshot:
        live_nodes = self.live_nodes
        failed_nodes = tuple(node for node in self.nodes if node not in self._live_nodes)
        snapshot = PeerLivenessSnapshot(
            nodes=self.nodes,
            live_nodes=live_nodes,
            failed_nodes=failed_nodes,
            is_degraded=len(live_nodes) < len(self.nodes),
        )
        self.graph.publish(self.routes.liveness_state, snapshot)
        return snapshot


@dataclass(frozen=True, slots=True)
class LeaderElectionSnapshot:
    term: int
    leader: str | None
    live_nodes: tuple[str, ...]
    quorum_size: int
    has_quorum: bool
    is_degraded: bool

    def __post_init__(self) -> None:
        _require_positive_int(self.term, "leader election term")
        if self.leader is not None:
            _require_non_empty_string(self.leader, "leader election leader")
        _require_node_set(self.live_nodes, "leader election live_nodes")
        _require_positive_int(self.quorum_size, "leader election quorum_size")
        if not isinstance(self.has_quorum, bool):
            raise TypeError("leader election has_quorum must be a boolean")
        if not isinstance(self.is_degraded, bool):
            raise TypeError("leader election is_degraded must be a boolean")


@dataclass(frozen=True, slots=True)
class LeaderElectionRoutes:
    membership: object
    node_failed: object
    election: object
    leader_state: object


class LeaderElectionSubgraph:
    """Reusable ManyFold subgraph for deterministic leader election."""

    def __init__(
        self,
        *,
        graph: Graph,
        routes: LeaderElectionRoutes,
        service: str,
        nodes: tuple[str, ...],
        peer_liveness: PeerLivenessSubgraph,
    ) -> None:
        _require_non_empty_string(service, "leader election service")
        self.graph = graph
        self.routes = routes
        self.service = service
        self.nodes = _require_node_set(nodes, "leader election nodes")
        if not isinstance(peer_liveness, PeerLivenessSubgraph):
            raise TypeError("leader election peer_liveness must be PeerLivenessSubgraph")
        if peer_liveness.nodes != self.nodes:
            raise ValueError("leader election nodes must match peer liveness nodes")
        self.peer_liveness = peer_liveness
        self._term = 0
        self._publish_membership()
        self.apply_liveness(peer_liveness.snapshot())

    @classmethod
    def install(
        cls,
        *,
        service: str,
        nodes: tuple[str, ...],
        peer_liveness: PeerLivenessSubgraph | None = None,
    ) -> "LeaderElectionSubgraph":
        resolved_liveness = (
            PeerLivenessSubgraph.install(service=service, nodes=nodes)
            if peer_liveness is None
            else peer_liveness
        )
        graph = Graph()
        routes = LeaderElectionRoutes(
            membership=route(
                owner=f"{service}_leader_election",
                family="membership",
                stream="members",
                schema=Schema.any("LeaderElectionMembership"),
            ),
            node_failed=route(
                owner=f"{service}_leader_election",
                family="membership",
                stream="node_failed",
                schema=Schema.any("LeaderElectionNodeFailed"),
            ),
            election=route(
                owner=f"{service}_leader_election",
                family="election",
                stream="result",
                schema=Schema.any("LeaderElectionResult"),
            ),
            leader_state=route(
                owner=f"{service}_leader_election",
                family="election",
                stream="leader_state",
                schema=Schema.any("LeaderElectionSnapshot"),
            ),
        )
        graph.register_diagram_node(
            f"{service}-leader-election",
            input_routes=(routes.membership, routes.node_failed),
            output_routes=(routes.election, routes.leader_state),
            group="leader election",
        )
        return cls(
            graph=graph,
            routes=routes,
            service=service,
            nodes=nodes,
            peer_liveness=resolved_liveness,
        )

    @property
    def quorum_size(self) -> int:
        return len(self.nodes) // 2 + 1

    @property
    def leader(self) -> str | None:
        return self.snapshot().leader

    @property
    def live_nodes(self) -> tuple[str, ...]:
        return self.snapshot().live_nodes

    def kill_node(self, node: str) -> LeaderElectionSnapshot:
        liveness = self.peer_liveness.mark_failed(node)
        self.graph.publish(self.routes.node_failed, node)
        return self.apply_liveness(liveness)

    def apply_liveness(
        self,
        liveness: PeerLivenessSnapshot,
    ) -> LeaderElectionSnapshot:
        if not isinstance(liveness, PeerLivenessSnapshot):
            raise TypeError("leader election liveness must be PeerLivenessSnapshot")
        if liveness.nodes != self.nodes:
            raise ValueError("leader election liveness nodes must match")
        return self._elect(liveness)

    def snapshot(self) -> LeaderElectionSnapshot:
        latest = self.graph.latest(self.routes.leader_state)
        if latest is None:
            raise RuntimeError("leader election has not published leader state")
        return latest.value

    def require_writable_quorum(self) -> LeaderElectionSnapshot:
        snapshot = self.snapshot()
        if not snapshot.has_quorum:
            raise RuntimeError(
                "leader election cannot accept writes without quorum: "
                f"live={len(snapshot.live_nodes)} required={snapshot.quorum_size}"
            )
        if snapshot.leader is None:
            raise RuntimeError("leader election has quorum but no leader")
        return snapshot

    def _publish_membership(self) -> None:
        self.graph.publish(self.routes.membership, self.nodes)

    def _elect(self, liveness: PeerLivenessSnapshot) -> LeaderElectionSnapshot:
        self._term += 1
        has_quorum = len(liveness.live_nodes) >= self.quorum_size
        leader = min(liveness.live_nodes) if has_quorum else None
        snapshot = LeaderElectionSnapshot(
            term=self._term,
            leader=leader,
            live_nodes=liveness.live_nodes,
            quorum_size=self.quorum_size,
            has_quorum=has_quorum,
            is_degraded=liveness.is_degraded,
        )
        self.graph.publish(self.routes.election, snapshot)
        self.graph.publish(self.routes.leader_state, snapshot)
        return snapshot


@dataclass(frozen=True, slots=True)
class ReplicationHealthSnapshot:
    nodes: tuple[str, ...]
    live_nodes: tuple[str, ...]
    in_sync_nodes: tuple[str, ...]
    required_acks: int
    can_accept_writes: bool
    is_degraded: bool

    def __post_init__(self) -> None:
        nodes = set(_require_node_set(self.nodes, "replication nodes"))
        live_nodes = set(_require_node_set(self.live_nodes, "replication live_nodes"))
        in_sync_nodes = set(
            _require_node_set(self.in_sync_nodes, "replication in_sync_nodes")
        )
        _require_positive_int(self.required_acks, "replication required_acks")
        if not live_nodes.issubset(nodes):
            raise ValueError("replication live_nodes must be declared nodes")
        if not in_sync_nodes.issubset(live_nodes):
            raise ValueError("replication in_sync_nodes must be live nodes")
        if not isinstance(self.can_accept_writes, bool):
            raise TypeError("replication can_accept_writes must be a boolean")
        if not isinstance(self.is_degraded, bool):
            raise TypeError("replication is_degraded must be a boolean")


@dataclass(frozen=True, slots=True)
class ReplicationHealthRoutes:
    replica_offsets: object
    liveness_applied: object
    replication_state: object


class ReplicationHealthSubgraph:
    """Reusable ManyFold subgraph for ISR and required-acks write gating."""

    def __init__(
        self,
        *,
        graph: Graph,
        routes: ReplicationHealthRoutes,
        service: str,
        nodes: tuple[str, ...],
        required_acks: int,
        peer_liveness: PeerLivenessSubgraph,
    ) -> None:
        _require_non_empty_string(service, "replication service")
        self.graph = graph
        self.routes = routes
        self.service = service
        self.nodes = _require_node_set(nodes, "replication nodes")
        _require_positive_int(required_acks, "required_acks")
        if required_acks > len(nodes):
            raise ValueError("required_acks cannot exceed replication node count")
        if not isinstance(peer_liveness, PeerLivenessSubgraph):
            raise TypeError("replication peer_liveness must be PeerLivenessSubgraph")
        if peer_liveness.nodes != self.nodes:
            raise ValueError("replication nodes must match peer liveness nodes")
        self.required_acks = required_acks
        self.peer_liveness = peer_liveness
        self._offsets = {node: 0 for node in nodes}
        self.apply_liveness(peer_liveness.snapshot())

    @classmethod
    def install(
        cls,
        *,
        service: str,
        nodes: tuple[str, ...],
        required_acks: int,
        peer_liveness: PeerLivenessSubgraph,
    ) -> "ReplicationHealthSubgraph":
        graph = Graph()
        routes = ReplicationHealthRoutes(
            replica_offsets=route(
                owner=f"{service}_replication_health",
                family="replication",
                stream="replica_offsets",
                schema=Schema.any("ReplicationReplicaOffsets"),
            ),
            liveness_applied=route(
                owner=f"{service}_replication_health",
                family="replication",
                stream="liveness_applied",
                schema=Schema.any("ReplicationLivenessApplied"),
            ),
            replication_state=route(
                owner=f"{service}_replication_health",
                family="replication",
                stream="replication_state",
                schema=Schema.any("ReplicationHealthSnapshot"),
            ),
        )
        graph.register_diagram_node(
            f"{service}-replication-health",
            input_routes=(routes.replica_offsets, routes.liveness_applied),
            output_routes=(routes.replication_state,),
            group="replication health",
        )
        return cls(
            graph=graph,
            routes=routes,
            service=service,
            nodes=nodes,
            required_acks=required_acks,
            peer_liveness=peer_liveness,
        )

    def record_replication(
        self,
        *,
        offset: int,
        replicated_nodes: tuple[str, ...],
    ) -> ReplicationHealthSnapshot:
        _require_positive_int(offset, "replication offset")
        replicated = _require_node_set(replicated_nodes, "replicated_nodes")
        live_nodes = set(self.peer_liveness.snapshot().live_nodes)
        for node in replicated:
            if node not in live_nodes:
                raise RuntimeError(f"cannot replicate to non-live node: {node}")
            self._offsets[node] = offset
        self.graph.publish(self.routes.replica_offsets, (offset, replicated))
        return self._publish_snapshot()

    def apply_liveness(
        self,
        liveness: PeerLivenessSnapshot,
    ) -> ReplicationHealthSnapshot:
        if not isinstance(liveness, PeerLivenessSnapshot):
            raise TypeError("replication liveness must be PeerLivenessSnapshot")
        if liveness.nodes != self.nodes:
            raise ValueError("replication liveness nodes must match")
        self.graph.publish(self.routes.liveness_applied, liveness)
        return self._publish_snapshot()

    def snapshot(self) -> ReplicationHealthSnapshot:
        latest = self.graph.latest(self.routes.replication_state)
        if latest is None:
            raise RuntimeError("replication health has not published state")
        return latest.value

    def require_writeable(self) -> ReplicationHealthSnapshot:
        snapshot = self.snapshot()
        if not snapshot.can_accept_writes:
            raise RuntimeError(
                "replication cannot accept writes without enough in-sync replicas: "
                f"in_sync={len(snapshot.in_sync_nodes)} required={snapshot.required_acks}"
            )
        return snapshot

    def _publish_snapshot(self) -> ReplicationHealthSnapshot:
        liveness = self.peer_liveness.snapshot()
        live_nodes = set(liveness.live_nodes)
        max_live_offset = max(
            (self._offsets[node] for node in live_nodes),
            default=0,
        )
        in_sync_nodes = tuple(
            sorted(
                node
                for node in live_nodes
                if self._offsets[node] == max_live_offset
            )
        )
        snapshot = ReplicationHealthSnapshot(
            nodes=self.nodes,
            live_nodes=liveness.live_nodes,
            in_sync_nodes=in_sync_nodes,
            required_acks=self.required_acks,
            can_accept_writes=len(in_sync_nodes) >= self.required_acks,
            is_degraded=len(liveness.live_nodes) < len(self.nodes),
        )
        self.graph.publish(self.routes.replication_state, snapshot)
        return snapshot


class SingleComputerRuntime:
    """Runtime that owns warm dormant ManyFold processes on one computer."""

    def __init__(
        self,
        name: str,
        *,
        root: str | Path,
        dormant_processes: int = _DEFAULT_DORMANT_PROCESS_COUNT,
        resources: ResourceObservation | None = None,
    ) -> None:
        _require_non_empty_string(name, "runtime name")
        _require_positive_int(dormant_processes, "dormant_processes")
        self._name = name
        self._root = Path(root)
        self._resources = ResourceObservation() if resources is None else resources
        self._slots: dict[int, FrozenProcess] = {
            slot: FrozenProcess(
                runtime=name,
                slot=slot,
                address=f"process://{name}/frozen-{slot}",
                state="dormant",
            )
            for slot in range(1, dormant_processes + 1)
        }
        self._active_by_name: dict[str, ProcessHandle] = {}

    @property
    def name(self) -> str:
        return self._name

    @property
    def root(self) -> Path:
        return self._root

    @property
    def resources(self) -> ResourceObservation:
        return self._resources

    def observe(self, resources: ResourceObservation) -> None:
        if not isinstance(resources, ResourceObservation):
            raise TypeError("runtime resources must be a ResourceObservation")
        self._resources = resources

    def shift(
        self,
        spec: ShiftSpec,
        *,
        snapshot: ProcessSnapshot | None = None,
    ) -> ShiftResult:
        if not isinstance(spec, ShiftSpec):
            raise TypeError("runtime shift spec must be a ShiftSpec")
        existing = self._active_by_name.get(spec.name)
        if existing is not None:
            if existing.capability != spec.capability:
                raise RuntimeError(
                    f"process {spec.name} already runs capability "
                    f"{existing.capability}, not {spec.capability}"
                )
            return ShiftResult(spec=spec, handle=existing, action="retain")

        frozen = self._claim_dormant_process()
        generation = 1 if snapshot is None else snapshot.generation + 1
        handle = ProcessHandle(
            runtime=self.name,
            slot=frozen.slot,
            name=spec.name,
            capability=spec.capability,
            address=f"process://{self.name}/{spec.name}",
            data_dir=_runtime_data_dir(self.root, spec.data_dir),
            generation=generation,
            proxy_from=() if snapshot is None else (snapshot.source_address,),
        )
        self._active_by_name[spec.name] = handle
        self._slots[frozen.slot] = FrozenProcess(
            runtime=self.name,
            slot=frozen.slot,
            address=frozen.address,
            state="active",
        )
        return ShiftResult(
            spec=spec,
            handle=handle,
            action="shift" if snapshot is None else "move",
            snapshot=snapshot,
        )

    def snapshot_and_release(self, name: str) -> ProcessSnapshot:
        _require_non_empty_string(name, "process name")
        try:
            handle = self._active_by_name.pop(name)
        except KeyError as error:
            raise RuntimeError(
                f"runtime {self.name} has no active process {name}"
            ) from error
        self._slots[handle.slot] = FrozenProcess(
            runtime=self.name,
            slot=handle.slot,
            address=f"process://{self.name}/frozen-{handle.slot}",
            state="dormant",
        )
        return ProcessSnapshot(
            process=handle.name,
            capability=handle.capability,
            source_runtime=handle.runtime,
            source_address=handle.address,
            generation=handle.generation,
        )

    def require_process(self, name: str) -> ProcessHandle:
        _require_non_empty_string(name, "process name")
        try:
            return self._active_by_name[name]
        except KeyError as error:
            raise RuntimeError(
                f"runtime {self.name} has no active process {name}"
            ) from error

    def dormant_processes(self) -> tuple[FrozenProcess, ...]:
        return tuple(
            slot
            for slot in self._slots.values()
            if slot.state == "dormant"
        )

    def spawn_dormant_processes(self, count: int) -> tuple[FrozenProcess, ...]:
        _require_positive_int(count, "spawn dormant process count")
        start = max(self._slots, default=0) + 1
        spawned = tuple(
            FrozenProcess(
                runtime=self.name,
                slot=slot,
                address=f"process://{self.name}/frozen-{slot}",
                state="dormant",
            )
            for slot in range(start, start + count)
        )
        self._slots.update((process.slot, process) for process in spawned)
        return spawned

    def process_handles(self) -> tuple[ProcessHandle, ...]:
        return tuple(self._active_by_name[name] for name in sorted(self._active_by_name))

    def _claim_dormant_process(self) -> FrozenProcess:
        try:
            return min(self.dormant_processes(), key=lambda slot: slot.slot)
        except ValueError as error:
            raise RuntimeError(f"runtime {self.name} has no dormant processes") from error


class RuntimeCluster:
    """Runtime made from a list of single-computer runtimes."""

    def __init__(self, runtimes: tuple[SingleComputerRuntime, ...]) -> None:
        if not runtimes:
            raise ValueError("runtime cluster must contain at least one runtime")
        self._runtimes: dict[str, SingleComputerRuntime] = {}
        for runtime in runtimes:
            if not isinstance(runtime, SingleComputerRuntime):
                raise TypeError("runtime cluster entries must be SingleComputerRuntime")
            if runtime.name in self._runtimes:
                raise ValueError(f"duplicate runtime name: {runtime.name}")
            self._runtimes[runtime.name] = runtime

    def runtime(self, name: str) -> SingleComputerRuntime:
        _require_non_empty_string(name, "runtime name")
        try:
            return self._runtimes[name]
        except KeyError as error:
            raise RuntimeError(f"unknown runtime: {name}") from error

    def add_runtime(self, runtime: SingleComputerRuntime) -> None:
        if not isinstance(runtime, SingleComputerRuntime):
            raise TypeError("runtime cluster entry must be a SingleComputerRuntime")
        if runtime.name in self._runtimes:
            raise ValueError(f"duplicate runtime name: {runtime.name}")
        self._runtimes[runtime.name] = runtime

    def runtimes(self) -> tuple[SingleComputerRuntime, ...]:
        return tuple(self._runtimes[name] for name in sorted(self._runtimes))

    def shift(self, spec: ShiftSpec) -> ShiftResult:
        if not isinstance(spec, ShiftSpec):
            raise TypeError("cluster shift spec must be a ShiftSpec")
        existing_runtime = self._runtime_with_process(spec.name)
        if existing_runtime is not None:
            return existing_runtime.shift(spec)
        return self._select_runtime(excluding=()).shift(spec)

    def shift_many(self, specs: tuple[ShiftSpec, ...]) -> tuple[ShiftResult, ...]:
        if not isinstance(specs, tuple):
            raise TypeError("cluster shift specs must be a tuple")
        return tuple(self.shift(spec) for spec in specs)

    def observe(self, runtime: str, resources: ResourceObservation) -> None:
        self.runtime(runtime).observe(resources)

    def move(self, process: str) -> ShiftResult:
        _require_non_empty_string(process, "process name")
        source = self._runtime_with_process(process)
        if source is None:
            raise RuntimeError(f"cluster has no active process {process}")
        current = source.require_process(process)
        target = self._select_runtime(excluding=(source.name,))
        snapshot = source.snapshot_and_release(process)
        return target.shift(
            ShiftSpec(
                name=current.name,
                capability=current.capability,
                data_dir=current.data_dir.name
                if current.data_dir.is_absolute()
                else current.data_dir,
            ),
            snapshot=snapshot,
        )

    def process_handles(self) -> tuple[ProcessHandle, ...]:
        return tuple(
            handle
            for runtime_name in sorted(self._runtimes)
            for handle in self._runtimes[runtime_name].process_handles()
        )

    def runtime_names(self) -> tuple[str, ...]:
        return tuple(sorted(self._runtimes))

    def dormant_process_count(self) -> int:
        return sum(len(runtime.dormant_processes()) for runtime in self._runtimes.values())

    def _runtime_with_process(self, process: str) -> SingleComputerRuntime | None:
        for runtime in self._runtimes.values():
            if any(handle.name == process for handle in runtime.process_handles()):
                return runtime
        return None

    def _select_runtime(self, *, excluding: tuple[str, ...]) -> SingleComputerRuntime:
        excluded = set(excluding)
        candidates = tuple(
            runtime
            for runtime in self._runtimes.values()
            if runtime.name not in excluded and runtime.dormant_processes()
        )
        if not candidates:
            raise RuntimeError("cluster has no runtime with dormant process capacity")
        return min(
            candidates,
            key=lambda runtime: (
                runtime.resources.pressure,
                len(runtime.process_handles()),
                runtime.name,
            ),
        )


class ManyFoldController:
    """Controller that tracks runtimes and maintains dormant process capacity."""

    def __init__(
        self,
        name: str,
        *,
        primary: SingleComputerRuntime,
        policy: ManyFoldControllerPolicy | None = None,
    ) -> None:
        _require_non_empty_string(name, "controller name")
        if not isinstance(primary, SingleComputerRuntime):
            raise TypeError("controller primary must be a SingleComputerRuntime")
        self._name = name
        self._policy = ManyFoldControllerPolicy() if policy is None else policy
        if not isinstance(self._policy, ManyFoldControllerPolicy):
            raise TypeError("controller policy must be a ManyFoldControllerPolicy")
        self._cluster = RuntimeCluster((primary,))
        self.ensure_dormant_buffer()

    @property
    def name(self) -> str:
        return self._name

    @property
    def cluster(self) -> RuntimeCluster:
        return self._cluster

    @property
    def policy(self) -> ManyFoldControllerPolicy:
        return self._policy

    def attach_runtime(self, runtime: SingleComputerRuntime) -> None:
        self._cluster.add_runtime(runtime)
        self.ensure_dormant_buffer()

    def shift(self, spec: ShiftSpec) -> ShiftResult:
        self.ensure_dormant_buffer(additional_needed=1)
        result = self._cluster.shift(spec)
        self.ensure_dormant_buffer()
        return result

    def shift_many(self, specs: tuple[ShiftSpec, ...]) -> tuple[ShiftResult, ...]:
        if not isinstance(specs, tuple):
            raise TypeError("controller shift specs must be a tuple")
        return tuple(self.shift(spec) for spec in specs)

    def observe(self, runtime: str, resources: ResourceObservation) -> None:
        self._cluster.observe(runtime, resources)

    def move(self, process: str) -> ShiftResult:
        result = self._cluster.move(process)
        self.ensure_dormant_buffer()
        return result

    def snapshot(self) -> ManyFoldControllerSnapshot:
        return ManyFoldControllerSnapshot(
            controller=self.name,
            runtimes=self._cluster.runtime_names(),
            active_processes=len(self._cluster.process_handles()),
            dormant_processes=self._cluster.dormant_process_count(),
            dormant_buffer=self.policy.dormant_buffer,
            max_processes=self.policy.max_processes,
        )

    def ensure_dormant_buffer(self, *, additional_needed: int = 0) -> None:
        _require_non_negative_int(additional_needed, "controller additional_needed")
        target = self.policy.dormant_buffer + additional_needed
        while self._cluster.dormant_process_count() < target:
            current = self._cluster.dormant_process_count()
            total = len(self._cluster.process_handles()) + current
            remaining_capacity = self.policy.max_processes - total
            if remaining_capacity <= 0:
                raise RuntimeError(
                    "controller cannot satisfy dormant process buffer: "
                    f"dormant={current} target={target} "
                    f"active={len(self._cluster.process_handles())} "
                    f"max={self.policy.max_processes}"
                )
            spawn_count = min(
                self.policy.spawn_batch_size,
                target - current,
                remaining_capacity,
            )
            self._select_spawn_runtime().spawn_dormant_processes(spawn_count)

    def _select_spawn_runtime(self) -> SingleComputerRuntime:
        return min(
            self._cluster.runtimes(),
            key=lambda runtime: (
                runtime.resources.pressure,
                len(runtime.dormant_processes()),
                runtime.name,
            ),
        )


class ShiftedKafkaCluster:
    """Kafka-shaped request path built from shifted dormant processes."""

    def __init__(
        self,
        *,
        brokers: tuple[ProcessHandle, ...],
        graph_routing: bool = False,
        durable_root: Path | None = None,
        required_acks: int = 2,
    ) -> None:
        if not isinstance(brokers, tuple) or not brokers:
            raise ValueError("Kafka cluster brokers must be a non-empty tuple")
        for broker in brokers:
            _require_capability(broker, "kafka.broker")
        self.brokers = brokers
        self._live_brokers = {broker.name: broker for broker in brokers}
        self.peer_liveness = PeerLivenessSubgraph.install(
            service="kafka",
            nodes=tuple(broker.name for broker in brokers),
        )
        self.leader_election = LeaderElectionSubgraph.install(
            service="kafka",
            nodes=tuple(broker.name for broker in brokers),
            peer_liveness=self.peer_liveness,
        )
        self.replication_health = ReplicationHealthSubgraph.install(
            service="kafka",
            nodes=tuple(broker.name for broker in brokers),
            required_acks=required_acks,
            peer_liveness=self.peer_liveness,
        )
        self._next_ingress = 0
        self._request_count = 0
        self._replica_offsets = {broker.name: 0 for broker in brokers}
        self.input = KafkaInputs(
            event_stream=InputPort(
                owner="kafka",
                name="event_stream",
                handler=self.produce,
            )
        )
        self.output = KafkaOutputs(
            records=OutputPort(owner="kafka", name="records"),
        )
        self._graph_route = _kafka_benchmark_graph_route() if graph_routing else None
        self._graph = Graph() if graph_routing else None
        self._durable_logs = (
            None
            if durable_root is None
            else KafkaDurableReplicaLogs(root=durable_root, brokers=brokers)
        )

    @classmethod
    def from_shifts(
        cls,
        shifts: tuple[ShiftResult, ...],
        *,
        graph_routing: bool = False,
        durable_root: Path | None = None,
        required_acks: int = 2,
    ) -> "ShiftedKafkaCluster":
        handles = tuple(shift.handle for shift in shifts)
        return cls(
            brokers=tuple(
                handle for handle in handles if handle.capability == "kafka.broker"
            ),
            graph_routing=graph_routing,
            durable_root=durable_root,
            required_acks=required_acks,
        )

    @property
    def request_count(self) -> int:
        return self._request_count

    @property
    def leader(self) -> ProcessHandle | None:
        leader_name = self.leader_election.leader
        if leader_name is None:
            return None
        return self._live_brokers[leader_name]

    @property
    def live_brokers(self) -> tuple[ProcessHandle, ...]:
        return tuple(
            self._live_brokers[name]
            for name in sorted(self._live_brokers)
        )

    @property
    def quorum_size(self) -> int:
        return self.leader_election.quorum_size

    @property
    def is_degraded(self) -> bool:
        return self.leader_election.snapshot().is_degraded

    def kill_broker(self, broker_name: str) -> None:
        _require_non_empty_string(broker_name, "broker name")
        if broker_name not in self._live_brokers:
            raise RuntimeError(f"Kafka broker is not live: {broker_name}")
        del self._live_brokers[broker_name]
        liveness = self.peer_liveness.mark_failed(broker_name)
        self.leader_election.apply_liveness(liveness)
        self.replication_health.apply_liveness(liveness)

    def produce(self, payload: bytes) -> int:
        if not isinstance(payload, bytes):
            raise TypeError("Kafka payload must be bytes")
        if not payload:
            raise ValueError("Kafka payload must be non-empty")
        leader_snapshot = self.leader_election.require_writable_quorum()
        self.replication_health.require_writeable()
        live_brokers = self.live_brokers
        self._next_ingress = (self._next_ingress + 1) % len(live_brokers)
        offset = self._request_count
        self._request_count += 1
        if self._graph is not None and self._graph_route is not None:
            self._graph.publish(self._graph_route, payload)
            latest = self._graph.latest(self._graph_route)
            if latest is None:
                raise RuntimeError("Kafka graph route did not retain latest payload")
        if self._durable_logs is not None:
            self._durable_logs.append(
                offset,
                payload,
                broker_names=tuple(broker.name for broker in live_brokers),
            )
        for broker in live_brokers:
            self._replica_offsets[broker.name] = offset + 1
        self.replication_health.record_replication(
            offset=offset + 1,
            replicated_nodes=tuple(broker.name for broker in live_brokers),
        )
        if leader_snapshot.leader is None:
            raise RuntimeError("Kafka leader election returned no leader")
        self.output.records.emit(
            KafkaRecord(
                offset=offset,
                payload=payload,
                leader=leader_snapshot.leader,
            )
        )
        return offset

    def require_fully_replicated(self) -> None:
        for broker in self.live_brokers:
            if self._replica_offsets[broker.name] != self._request_count:
                raise RuntimeError(f"Kafka broker is behind: {broker.name}")

    def close(self) -> None:
        if self._durable_logs is not None:
            self._durable_logs.close()

    def _require_writable_quorum(self) -> None:
        self.leader_election.require_writable_quorum()
        self.replication_health.require_writeable()


@dataclass(frozen=True, slots=True)
class ServiceDiscoveryRegistration:
    service: str
    endpoint: str
    broker_names: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_non_empty_string(self.service, "service discovery service")
        _require_non_empty_string(self.endpoint, "service discovery endpoint")
        _require_node_set(self.broker_names, "service discovery broker_names")


class ServiceDiscoveryBinding:
    """Service discovery wrapper that publishes a service without being owned by it."""

    def __init__(self, process: ProcessHandle) -> None:
        _require_capability(process, "service.discovery")
        self.process = process
        self._registrations: dict[str, ServiceDiscoveryRegistration] = {}
        self.registration_stream = ManyFoldStream(
            owner="service_discovery",
            family="registry",
            stream=f"{process.name}_registration",
            schema_name="ServiceDiscoveryRegistration",
        )
        self.lookup_stream = ManyFoldStream(
            owner="service_discovery",
            family="registry",
            stream=f"{process.name}_lookup",
            schema_name="ServiceDiscoveryLookup",
        )

    def publish(
        self,
        *,
        service: str,
        endpoint: str,
        cluster: ShiftedKafkaCluster,
    ) -> ServiceDiscoveryRegistration:
        _require_non_empty_string(service, "service discovery service")
        _require_non_empty_string(endpoint, "service discovery endpoint")
        if not isinstance(cluster, ShiftedKafkaCluster):
            raise TypeError("service discovery cluster must be ShiftedKafkaCluster")
        registration = ServiceDiscoveryRegistration(
            service=service,
            endpoint=endpoint,
            broker_names=tuple(broker.name for broker in cluster.brokers),
        )
        self._registrations[service] = registration
        self.registration_stream.publish(registration)
        return registration

    def require(self, service: str) -> ServiceDiscoveryRegistration:
        _require_non_empty_string(service, "service discovery service")
        self.lookup_stream.publish(service)
        try:
            return self._registrations[service]
        except KeyError as error:
            raise RuntimeError(f"service is not discovered: {service}") from error


class LoadBalancerBinding:
    """Ingress wrapper that routes to a service without being owned by it."""

    def __init__(self, process: ProcessHandle) -> None:
        _require_capability(process, "ingress.load_balance")
        self.process = process
        self.input = LoadBalancerInputs(
            application=InputPort(
                owner="load_balancer",
                name=f"{process.name}_application",
                handler=self._route_application,
            )
        )
        self.output = LoadBalancerOutputs(
            application=OutputPort(
                owner="load_balancer",
                name=f"{process.name}_application",
            )
        )
        self.routed_stream = ManyFoldStream(
            owner="load_balancer",
            family="ingress",
            stream=f"{process.name}_routed",
            schema_name="LoadBalancerRoutedResponse",
        )

    @property
    def endpoint(self) -> str:
        return self.process.address

    def _route_application(self, payload: bytes) -> int:
        responses = self.output.application.emit(payload)
        if len(responses) != 1:
            raise RuntimeError(
                f"load balancer expected one application response, got {len(responses)}"
            )
        offset = responses[0]
        if isinstance(offset, bool) or not isinstance(offset, int):
            raise RuntimeError("load balancer application response must be an integer offset")
        self.routed_stream.publish(offset)
        return offset


class KafkaProducerBinding:
    """Producer-side stream facade for Kafka produce requests."""

    def __init__(self, process: ProcessHandle) -> None:
        if not isinstance(process, ProcessHandle):
            raise TypeError("Kafka producer process must be a ProcessHandle")
        if process.capability not in ("kafka.producer", "manyfold.client"):
            raise ValueError(
                f"process {process.name} must provide kafka.producer, not {process.capability}"
            )
        self.process = process
        self.output = KafkaProducerOutputs(
            produce=OutputPort(owner=process.name, name="produce"),
        )

    def contribute_runtime(
        self,
        cluster: RuntimeCluster,
        *,
        root: str | Path | None = None,
        dormant_processes: int = _DEFAULT_DORMANT_PROCESS_COUNT,
        resources: ResourceObservation | None = None,
    ) -> SingleComputerRuntime:
        if not isinstance(cluster, RuntimeCluster):
            raise TypeError("client runtime contribution cluster must be RuntimeCluster")
        runtime = SingleComputerRuntime(
            f"{self.process.name}-runtime",
            root=self.process.data_dir if root is None else root,
            dormant_processes=dormant_processes,
            resources=resources,
        )
        cluster.add_runtime(runtime)
        return runtime

    def discover_coordination(
        self,
        *,
        discovery: ServiceDiscoveryBinding,
        topology: NetworkTopologyBinding,
        service: str = "kafka",
    ) -> KafkaCoordination:
        return _discover_kafka_coordination(
            discovery=discovery,
            topology=topology,
            service=service,
        )

    def produce(self, payload: bytes) -> int:
        responses = self.output.produce.emit(payload)
        if len(responses) != 1:
            raise RuntimeError(
                f"Kafka producer expected one produce response, got {len(responses)}"
            )
        offset = responses[0]
        if isinstance(offset, bool) or not isinstance(offset, int):
            raise RuntimeError("Kafka producer response must be an integer offset")
        return offset


KafkaClientBinding = KafkaProducerBinding


class KafkaConsumerBinding:
    """Consumer-side stream facade for Kafka records."""

    def __init__(self, process: ProcessHandle) -> None:
        _require_capability(process, "kafka.consumer")
        self.process = process
        self.input = KafkaConsumerInputs(
            records=InputPort(
                owner=process.name,
                name="records",
                handler=self.consume,
            )
        )
        self._records: dict[int, KafkaRecord] = {}

    def consume(self, record: KafkaRecord) -> int:
        if not isinstance(record, KafkaRecord):
            raise TypeError("Kafka consumer record must be a KafkaRecord")
        self._records[record.offset] = record
        return record.offset

    def require_consumed(self, offset: int) -> KafkaRecord:
        _require_non_negative_int(offset, "Kafka consumed offset")
        try:
            return self._records[offset]
        except KeyError as error:
            raise RuntimeError(f"Kafka consumer has not received offset {offset}") from error

    def discover_coordination(
        self,
        *,
        discovery: ServiceDiscoveryBinding,
        topology: NetworkTopologyBinding,
        service: str = "kafka",
    ) -> KafkaCoordination:
        return _discover_kafka_coordination(
            discovery=discovery,
            topology=topology,
            service=service,
        )


class NetworkTopologyBinding:
    """Dependency injection service that wires runtime resources into a network."""

    def __init__(self, process: ProcessHandle) -> None:
        _require_capability(process, "network.topology")
        self.process = process
        self.configuration_stream = ManyFoldStream(
            owner="network_topology",
            family="configuration",
            stream=f"{process.name}_configured",
            schema_name="NetworkTopologyConfiguration",
        )

    def configure_kafka(
        self,
        *,
        service: str,
        discovery: ServiceDiscoveryBinding,
        ingress: LoadBalancerBinding,
        kafka: ShiftedKafkaCluster,
        producer: KafkaProducerBinding,
        consumer: KafkaConsumerBinding,
        producer_transport: StreamTransport | None = None,
        application_transport: StreamTransport | None = None,
        consumer_transport: StreamTransport | None = None,
    ) -> NetworkTopologyConfiguration:
        _require_non_empty_string(service, "network topology service")
        if not isinstance(discovery, ServiceDiscoveryBinding):
            raise TypeError("network topology discovery must be ServiceDiscoveryBinding")
        if not isinstance(ingress, LoadBalancerBinding):
            raise TypeError("network topology ingress must be LoadBalancerBinding")
        if not isinstance(kafka, ShiftedKafkaCluster):
            raise TypeError("network topology kafka must be ShiftedKafkaCluster")
        if not isinstance(producer, KafkaProducerBinding):
            raise TypeError("network topology producer must be KafkaProducerBinding")
        if not isinstance(consumer, KafkaConsumerBinding):
            raise TypeError("network topology consumer must be KafkaConsumerBinding")
        discovery.publish(service=service, endpoint=ingress.endpoint, cluster=kafka)
        ingress.output.application.connect(
            kafka.input.event_stream,
            transport=application_transport,
        )
        producer.output.produce.connect(
            ingress.input.application,
            transport=producer_transport,
        )
        kafka.output.records.connect(
            consumer.input.records,
            transport=consumer_transport,
        )
        configuration = NetworkTopologyConfiguration(
            service=service,
            endpoint=ingress.endpoint,
            edges=(
                (producer.output.produce.stream.name, ingress.input.application.stream.name),
                (ingress.output.application.stream.name, kafka.input.event_stream.stream.name),
                (kafka.output.records.stream.name, consumer.input.records.stream.name),
            ),
        )
        self.configuration_stream.publish(configuration)
        return configuration


class KafkaProduceStreamBinding:
    """Kafka produce protocol carried over a reusable TCP byte stream."""

    def __init__(self, ingress: LoadBalancerBinding) -> None:
        if not isinstance(ingress, LoadBalancerBinding):
            raise TypeError("Kafka produce stream ingress must be LoadBalancerBinding")
        self.ingress = ingress
        self.input = KafkaProduceInputs(
            tcp_request=InputPort(
                owner="kafka_produce",
                name="tcp_request",
                handler=self.handle,
            )
        )
        self.request_stream = self.input.tcp_request.stream
        self.response_stream = ManyFoldStream(
            owner="kafka_produce",
            family="produce",
            stream="response",
            schema_name="KafkaProduceStreamResponse",
        )

    def handle(self, payload: bytes) -> bytes:
        offset = self.ingress.input.application.receive(payload)
        self.response_stream.publish(offset)
        return struct.pack(">Q", offset)

    def produce(self, client: TcpStreamClient, payload: bytes) -> int:
        if not isinstance(client, TcpStreamClient):
            raise TypeError("Kafka produce stream client must be TcpStreamClient")
        response = client.request(payload)
        if len(response) != 8:
            raise RuntimeError("Kafka produce stream returned invalid offset frame")
        return struct.unpack(">Q", response)[0]


class ManyFoldKafkaQueueClient:
    """Client facade that lazily provisions a Kafka-backed ManyFold queue."""

    def __init__(self, cluster: RuntimeCluster) -> None:
        if not isinstance(cluster, RuntimeCluster):
            raise TypeError("ManyFold Kafka queue cluster must be a RuntimeCluster")
        self.cluster = cluster
        self._shifts: tuple[ShiftResult, ...] = ()
        self._kafka: ShiftedKafkaCluster | None = None
        self._producer: KafkaProducerBinding | None = None
        self._consumer: KafkaConsumerBinding | None = None

    @property
    def shifted_processes(self) -> int:
        return len(self._shifts)

    def send(self, payload: bytes) -> KafkaQueueAck:
        if not isinstance(payload, bytes) or not payload:
            raise ValueError("ManyFold Kafka queue payload must be non-empty bytes")
        kafka, producer = self._ensure_queue()
        offset = producer.produce(payload)
        kafka.require_fully_replicated()
        if self._consumer is None:
            raise RuntimeError("ManyFold Kafka queue has no consumer")
        consumed = self._consumer.require_consumed(offset)
        return KafkaQueueAck(
            payload=payload,
            produced_offset=offset,
            consumed_offset=consumed.offset,
            leader=consumed.leader,
            consumer=self._consumer.process.name,
        )

    def close(self) -> None:
        if self._kafka is not None:
            self._kafka.close()
            self._kafka = None

    def _ensure_queue(self) -> tuple[ShiftedKafkaCluster, KafkaProducerBinding]:
        if self._kafka is None or self._producer is None or self._consumer is None:
            self._shifts = self.cluster.shift_many(_kafka_topology_shift_specs())
            kafka, _, _, producer, consumer, _ = _compose_kafka_from_shifts(self._shifts)
            self._kafka = kafka
            self._producer = producer
            self._consumer = consumer
        return self._kafka, self._producer


class KafkaDurableReplicaLogs:
    """Append-only broker logs with fsync on every produce."""

    def __init__(self, *, root: Path, brokers: tuple[ProcessHandle, ...]) -> None:
        self._fds: dict[str, int] = {
            broker.name: self._open_log(root, broker) for broker in brokers
        }

    def append(
        self,
        offset: int,
        payload: bytes,
        *,
        broker_names: tuple[str, ...],
    ) -> None:
        _require_non_negative_int(offset, "offset")
        if not isinstance(payload, bytes):
            raise TypeError("durable Kafka payload must be bytes")
        if not broker_names:
            raise ValueError("durable Kafka broker_names must be non-empty")
        record = struct.pack(">QI", offset, len(payload)) + payload
        for broker_name in broker_names:
            fd = self._fds[broker_name]
            os.write(fd, record)
            os.fsync(fd)

    def close(self) -> None:
        while self._fds:
            _, fd = self._fds.popitem()
            os.close(fd)

    def _open_log(self, root: Path, broker: ProcessHandle) -> int:
        broker_dir = root / broker.name
        broker_dir.mkdir(parents=True, exist_ok=True)
        return os.open(
            broker_dir / "records.log",
            os.O_CREAT | os.O_APPEND | os.O_WRONLY,
            0o600,
        )


class TcpStreamServer:
    """Reusable framed TCP ingress stream."""

    def __init__(
        self,
        *,
        name: str,
        handler,
    ) -> None:
        _require_non_empty_string(name, "TCP stream name")
        if not callable(handler):
            raise TypeError("TCP stream handler must be callable")
        self.name = name
        self._server = _TcpThreadingServer(("127.0.0.1", 0), _TcpStreamHandler)
        self._server.stream_handler = handler
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name=f"manyfold-{name}-tcp",
            daemon=True,
        )

    @property
    def address(self) -> tuple[str, int]:
        host, port = self._server.server_address
        return str(host), int(port)

    def __enter__(self) -> "TcpStreamServer":
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5.0)

    def client(self) -> "TcpStreamClient":
        return TcpStreamClient(self.address)


class TcpStreamClient:
    """Reusable framed TCP egress stream."""

    def __init__(self, address: tuple[str, int]) -> None:
        host, port = address
        _require_non_empty_string(host, "TCP stream host")
        _require_positive_int(port, "TCP stream port")
        self.address = host, port

    def request(self, payload: bytes) -> bytes:
        if not isinstance(payload, bytes):
            raise TypeError("TCP stream payload must be bytes")
        with socket.create_connection(self.address, timeout=5.0) as client:
            client.sendall(struct.pack(">I", len(payload)) + payload)
            response_size = struct.unpack(">I", _recv_exact(client, 4))[0]
            return _recv_exact(client, response_size)


class RuntimeSubstrateResult(TypedDict):
    controller: str
    single_runtime: str
    cluster_runtimes: tuple[str, ...]
    dormant_processes: int
    controller_dormant_buffer: int
    controller_max_processes: int
    shifted: tuple[dict[str, str], ...]
    actions: tuple[ShiftAction, ...]
    moved: dict[str, str]


class KafkaDormantBootstrapBenchmarkResult(TypedDict):
    mode: str
    requests: int
    shifted_processes: int
    broker_count: int
    leader: str
    boot_seconds: float
    request_seconds: float
    total_seconds: float
    average_request_us: float
    dormant_processes_remaining: int


class KafkaLeaderFailureResult(TypedDict):
    initial_leader: str
    killed_leader: str
    re_elected_leader: str | None
    live_after_leader_kill: tuple[str, ...]
    in_sync_after_leader_kill: tuple[str, ...]
    required_acks: int
    degraded_after_leader_kill: bool
    continued_offsets: tuple[int, ...]
    request_count_after_recovery: int
    killed_second_broker: str
    leader_after_quorum_loss: str | None
    live_after_quorum_loss: tuple[str, ...]
    in_sync_after_quorum_loss: tuple[str, ...]
    replication_can_write_after_quorum_loss: bool
    quorum_lost_error: str


class KafkaQueueProgramResult(TypedDict):
    messages: int
    interval_seconds: float
    acks: tuple[str, ...]
    shifted_processes: int


class PubSubUpgradeResult(TypedDict):
    topic: str
    payload: str
    pubsub_backing: str
    pubsub_delivered_to: tuple[str, ...]
    pubsub_replayable: bool
    durable_backing: str
    durable_delivered_to: tuple[str, ...]
    durable_replayable: bool
    durable_components: tuple[str, ...]
    durable_required_acks: int
    durable_replica_count: int
    durable_shifted_processes: int


class ProjectionIntoResult(TypedDict):
    projection: str
    source_topic: str
    output_topic: str
    delivered_to: str
    payload: str
    local_stream_messages: int


@dataclass(frozen=True, slots=True)
class KafkaQueueAck:
    payload: bytes
    produced_offset: int
    consumed_offset: int
    leader: str
    consumer: str

    def __post_init__(self) -> None:
        if not isinstance(self.payload, bytes) or not self.payload:
            raise ValueError("queue ack payload must be non-empty bytes")
        _require_non_negative_int(self.produced_offset, "queue ack produced_offset")
        _require_non_negative_int(self.consumed_offset, "queue ack consumed_offset")
        if self.produced_offset != self.consumed_offset:
            raise ValueError("queue ack produced and consumed offsets must match")
        _require_non_empty_string(self.leader, "queue ack leader")
        _require_non_empty_string(self.consumer, "queue ack consumer")

    def display(self) -> str:
        return (
            f"sent payload={self.payload.decode('ascii', 'backslashreplace')} "
            f"produced=offset:{self.produced_offset} "
            f"consumed=offset:{self.consumed_offset} "
            f"consumer={self.consumer} leader={self.leader}"
        )


@dataclass(frozen=True, slots=True)
class PubSubMessage:
    topic: str
    payload: bytes

    def __post_init__(self) -> None:
        _require_non_empty_string(self.topic, "pubsub topic")
        if not isinstance(self.payload, bytes) or not self.payload:
            raise ValueError("pubsub payload must be non-empty bytes")


@dataclass(frozen=True, slots=True)
class PubSubDelivery:
    topic: str
    payload: bytes
    backing: str
    delivered_to: tuple[str, ...]
    replayable: bool
    produced_offset: int | None = None
    consumed_offset: int | None = None

    def __post_init__(self) -> None:
        _require_non_empty_string(self.topic, "pubsub delivery topic")
        if not isinstance(self.payload, bytes) or not self.payload:
            raise ValueError("pubsub delivery payload must be non-empty bytes")
        _require_non_empty_string(self.backing, "pubsub delivery backing")
        if not isinstance(self.delivered_to, tuple):
            raise TypeError("pubsub delivered_to must be a tuple")
        for target in self.delivered_to:
            _require_non_empty_string(target, "pubsub delivery target")
        if not isinstance(self.replayable, bool):
            raise TypeError("pubsub replayable must be a bool")
        if self.produced_offset is not None:
            _require_non_negative_int(self.produced_offset, "pubsub produced_offset")
        if self.consumed_offset is not None:
            _require_non_negative_int(self.consumed_offset, "pubsub consumed_offset")


@dataclass(frozen=True, slots=True)
class DurableLogRecord:
    topic: str
    offset: int
    payload: bytes
    replicated_to: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_non_empty_string(self.topic, "durable log topic")
        _require_non_negative_int(self.offset, "durable log offset")
        if not isinstance(self.payload, bytes) or not self.payload:
            raise ValueError("durable log payload must be non-empty bytes")
        _require_node_set(self.replicated_to, "durable log replicated_to")


@dataclass(frozen=True, slots=True)
class ConsumerGroupCommit:
    group: str
    topic: str
    offset: int

    def __post_init__(self) -> None:
        _require_non_empty_string(self.group, "consumer group")
        _require_non_empty_string(self.topic, "consumer group topic")
        _require_non_negative_int(self.offset, "consumer group offset")


@dataclass(frozen=True, slots=True)
class ProjectionDelivery:
    projection: str
    source_topic: str
    output_topic: str
    delivered_to: str
    payload: bytes

    def __post_init__(self) -> None:
        _require_non_empty_string(self.projection, "projection")
        _require_non_empty_string(self.source_topic, "projection source topic")
        _require_non_empty_string(self.output_topic, "projection output topic")
        _require_non_empty_string(self.delivered_to, "projection delivered_to")
        if not isinstance(self.payload, bytes) or not self.payload:
            raise ValueError("projection payload must be non-empty bytes")


@dataclass(frozen=True, slots=True)
class DurabilityProfile:
    """Composable storage guarantees required by replayable services."""

    durable_log: bool
    replicated: bool
    required_acks: int
    replica_count: int

    def __post_init__(self) -> None:
        if not isinstance(self.durable_log, bool):
            raise TypeError("durability profile durable_log must be a bool")
        if not isinstance(self.replicated, bool):
            raise TypeError("durability profile replicated must be a bool")
        _require_positive_int(self.required_acks, "durability profile required_acks")
        _require_positive_int(self.replica_count, "durability profile replica_count")
        if self.required_acks > self.replica_count:
            raise ValueError("durability profile required_acks cannot exceed replicas")


class DurableLog:
    """Append/read contract for replicated replay storage."""

    profile: DurabilityProfile

    def append(self, *, topic: str, payload: bytes) -> DurableLogRecord:
        raise NotImplementedError

    def commit_consumer_offset(
        self,
        *,
        group: str,
        topic: str,
        offset: int,
    ) -> ConsumerGroupCommit:
        raise NotImplementedError


class ReplicatedDurableLog(DurableLog):
    """DurableLog backed by the current replicated Kafka-shaped queue topology."""

    def __init__(
        self,
        *,
        queue: ManyFoldKafkaQueueClient,
        profile: DurabilityProfile | None = None,
    ) -> None:
        if not isinstance(queue, ManyFoldKafkaQueueClient):
            raise TypeError("replicated durable log queue must be ManyFoldKafkaQueueClient")
        if profile is None:
            profile = DurabilityProfile(
                durable_log=True,
                replicated=True,
                required_acks=2,
                replica_count=3,
            )
        if not isinstance(profile, DurabilityProfile):
            raise TypeError("replicated durable log profile must be DurabilityProfile")
        if not profile.durable_log or not profile.replicated:
            raise ValueError("ReplicatedDurableLog requires durable and replicated storage")
        self.queue = queue
        self.profile = profile
        self._commits: dict[tuple[str, str], int] = {}

    @property
    def shifted_processes(self) -> int:
        return self.queue.shifted_processes

    def append(self, *, topic: str, payload: bytes) -> DurableLogRecord:
        _require_non_empty_string(topic, "durable log topic")
        if not isinstance(payload, bytes) or not payload:
            raise ValueError("durable log payload must be non-empty bytes")
        ack = self.queue.send(payload)
        return DurableLogRecord(
            topic=topic,
            offset=ack.produced_offset,
            payload=payload,
            replicated_to=tuple(
                f"replica-{index}" for index in range(self.profile.required_acks)
            ),
        )

    def commit_consumer_offset(
        self,
        *,
        group: str,
        topic: str,
        offset: int,
    ) -> ConsumerGroupCommit:
        commit = ConsumerGroupCommit(group=group, topic=topic, offset=offset)
        self._commits[(group, topic)] = offset
        return commit

    def close(self) -> None:
        self.queue.close()


class PubSubConnection:
    """Shared publish surface for live and durable pub/sub backends."""

    backing: str
    replayable: bool

    def publish(self, message: PubSubMessage) -> PubSubDelivery:
        raise NotImplementedError

    def close(self) -> None:
        return None


class ProjectionSink:
    """Target for projected stream records."""

    name: str

    def write(self, message: PubSubMessage) -> str:
        raise NotImplementedError


class LocalClientStreamSink(ProjectionSink):
    """Projection target that loads records back into a local client stream."""

    def __init__(self, *, name: str) -> None:
        self.name = _require_non_empty_string(name, "local client stream sink")
        self.input = TopicSubscriberInputs(
            messages=InputPort(owner=self.name, name="messages", handler=self._receive)
        )
        self._messages: list[PubSubMessage] = []

    @property
    def messages(self) -> tuple[PubSubMessage, ...]:
        return tuple(self._messages)

    def write(self, message: PubSubMessage) -> str:
        if not isinstance(message, PubSubMessage):
            raise TypeError("local client stream sink expected PubSubMessage")
        delivered_to = self.input.messages.receive(message)
        if not isinstance(delivered_to, str):
            raise TypeError("local client stream sink handler must return stream name")
        return delivered_to

    def _receive(self, message: PubSubMessage) -> str:
        if not isinstance(message, PubSubMessage):
            raise TypeError("local client stream expected PubSubMessage")
        self._messages.append(message)
        return self.name


class PubSubSink(ProjectionSink):
    """Projection target that writes records to any PubSub connection."""

    def __init__(self, *, connection: PubSubConnection) -> None:
        if not isinstance(connection, PubSubConnection):
            raise TypeError("pubsub sink connection must be PubSubConnection")
        self.connection = connection
        self.name = connection.backing

    def write(self, message: PubSubMessage) -> str:
        delivery = self.connection.publish(message)
        return delivery.backing


class StreamingProjection:
    """Typed streaming projection whose output target is chosen by into()."""

    def __init__(
        self,
        *,
        name: str,
        output_topic: str,
        transform: Callable[[PubSubMessage], bytes],
    ) -> None:
        self.name = _require_non_empty_string(name, "streaming projection")
        self.output_topic = _require_non_empty_string(
            output_topic, "streaming projection output topic"
        )
        if not callable(transform):
            raise TypeError("streaming projection transform must be callable")
        self._transform = transform

    def into(self, sink: ProjectionSink) -> "BoundStreamingProjection":
        if not isinstance(sink, ProjectionSink):
            raise TypeError("streaming projection sink must be ProjectionSink")
        return BoundStreamingProjection(projection=self, sink=sink)

    def project(self, message: PubSubMessage) -> PubSubMessage:
        if not isinstance(message, PubSubMessage):
            raise TypeError("streaming projection expected PubSubMessage")
        payload = self._transform(message)
        if not isinstance(payload, bytes) or not payload:
            raise ValueError("streaming projection transform must return non-empty bytes")
        return PubSubMessage(topic=self.output_topic, payload=payload)


class BoundStreamingProjection:
    """Executable projection bound to one concrete output sink."""

    def __init__(self, *, projection: StreamingProjection, sink: ProjectionSink) -> None:
        if not isinstance(projection, StreamingProjection):
            raise TypeError("bound projection requires StreamingProjection")
        if not isinstance(sink, ProjectionSink):
            raise TypeError("bound projection requires ProjectionSink")
        self.projection = projection
        self.sink = sink

    def consume(self, message: PubSubMessage) -> ProjectionDelivery:
        output = self.projection.project(message)
        delivered_to = self.sink.write(output)
        return ProjectionDelivery(
            projection=self.projection.name,
            source_topic=message.topic,
            output_topic=output.topic,
            delivered_to=delivered_to,
            payload=output.payload,
        )


class TopicSubscriberBinding:
    """Live topic subscriber with no replay or consumer-group contract."""

    def __init__(self, *, name: str) -> None:
        self.name = _require_non_empty_string(name, "topic subscriber name")
        self.input = TopicSubscriberInputs(
            messages=InputPort(owner=self.name, name="messages", handler=self.consume)
        )
        self._messages: list[PubSubMessage] = []

    @property
    def messages(self) -> tuple[PubSubMessage, ...]:
        return tuple(self._messages)

    def consume(self, message: PubSubMessage) -> str:
        if not isinstance(message, PubSubMessage):
            raise TypeError("topic subscriber expected PubSubMessage")
        self._messages.append(message)
        return self.name


class PubSub(PubSubConnection):
    """Ephemeral topic fanout: publish to live subscribers with MQTT symmetry."""

    backing = "pubsub"
    replayable = False

    def __init__(self) -> None:
        self._topics: dict[str, OutputPort] = {}

    def subscribe(self, topic: str, subscriber: TopicSubscriberBinding) -> None:
        if not isinstance(subscriber, TopicSubscriberBinding):
            raise TypeError("topic subscriber must be TopicSubscriberBinding")
        self._topic_output(topic).connect(subscriber.input.messages)

    def publish(self, message: PubSubMessage) -> PubSubDelivery:
        if not isinstance(message, PubSubMessage):
            raise TypeError("pubsub publish expected PubSubMessage")
        delivered_to = self._topic_output(message.topic).emit(message)
        return PubSubDelivery(
            topic=message.topic,
            payload=message.payload,
            backing=self.backing,
            delivered_to=tuple(str(target) for target in delivered_to),
            replayable=self.replayable,
        )

    def _topic_output(self, topic: str) -> OutputPort:
        topic = _require_non_empty_string(topic, "pubsub topic")
        output = self._topics.get(topic)
        if output is None:
            output = OutputPort(owner="pubsub", name=_stream_name_from_topic(topic))
            self._topics[topic] = output
        return output


class DurablePubSub(PubSubConnection):
    """Replayable topic fanout composed from DurableLog, ConsumerGroups, TopicRouting."""

    backing = "durable_pubsub"
    replayable = True
    components = ("DurableLog", "ConsumerGroups", "TopicRouting")

    def __init__(
        self,
        *,
        log: DurableLog,
        consumer_group: str = "default-consumer-group",
    ) -> None:
        if not isinstance(log, DurableLog):
            raise TypeError("DurablePubSub log must be DurableLog")
        self.log = log
        self.consumer_group = _require_non_empty_string(
            consumer_group, "DurablePubSub consumer group"
        )

    @property
    def shifted_processes(self) -> int:
        return getattr(self.log, "shifted_processes", 0)

    def publish(self, message: PubSubMessage) -> PubSubDelivery:
        if not isinstance(message, PubSubMessage):
            raise TypeError("durable pubsub publish expected PubSubMessage")
        record = self.log.append(topic=message.topic, payload=message.payload)
        commit = self.log.commit_consumer_offset(
            group=self.consumer_group,
            topic=message.topic,
            offset=record.offset,
        )
        return PubSubDelivery(
            topic=record.topic,
            payload=record.payload,
            backing=self.backing,
            delivered_to=(commit.group,),
            replayable=self.replayable,
            produced_offset=record.offset,
            consumed_offset=commit.offset,
        )

    def close(self) -> None:
        close = getattr(self.log, "close", None)
        if close is not None:
            close()


def run_example() -> RuntimeSubstrateResult:
    local = SingleComputerRuntime("laptop", root="/var/lib/manyfold/laptop")
    controller = ManyFoldController("manyfold-controller", primary=local)
    service_discovery = controller.shift(
        ShiftSpec(
            name="service-discovery",
            capability="service.discovery",
            data_dir=Path("service-discovery"),
        )
    )

    controller.attach_runtime(
        SingleComputerRuntime("worker-a", root="/var/lib/manyfold/worker-a")
    )
    controller.attach_runtime(
        SingleComputerRuntime("worker-b", root="/var/lib/manyfold/worker-b")
    )
    controller.observe("laptop", ResourceObservation(cpu=0.40, memory=0.50))
    controller.observe("worker-a", ResourceObservation(cpu=0.10, memory=0.20))
    controller.observe("worker-b", ResourceObservation(cpu=0.30, memory=0.10))

    shifted = (
        service_discovery,
        *controller.shift_many(
            (
                ShiftSpec(
                    name="load-balancer",
                    capability="ingress.load_balance",
                    data_dir=Path("load-balancer"),
                ),
                ShiftSpec(
                    name="kafka-broker-a",
                    capability="kafka.broker",
                    data_dir=Path("kafka/broker-a"),
                ),
                ShiftSpec(
                    name="kafka-broker-b",
                    capability="kafka.broker",
                    data_dir=Path("kafka/broker-b"),
                ),
                ShiftSpec(
                    name="kafka-broker-c",
                    capability="kafka.broker",
                    data_dir=Path("kafka/broker-c"),
                ),
            )
        ),
    )

    controller.observe("worker-a", ResourceObservation(cpu=0.95, memory=0.88))
    moved = controller.move("kafka-broker-a")
    cluster = controller.cluster
    controller_snapshot = controller.snapshot()

    return {
        "controller": controller_snapshot.controller,
        "single_runtime": local.name,
        "cluster_runtimes": cluster.runtime_names(),
        "dormant_processes": cluster.dormant_process_count(),
        "controller_dormant_buffer": controller_snapshot.dormant_buffer,
        "controller_max_processes": controller_snapshot.max_processes,
        "shifted": tuple(_shift_summary(shift) for shift in shifted),
        "actions": tuple(shift.action for shift in (*shifted, moved)),
        "moved": _shift_summary(moved),
    }


def run_kafka_leader_failure_example() -> KafkaLeaderFailureResult:
    cluster = RuntimeCluster(
        (
            SingleComputerRuntime("node-a", root="/var/lib/manyfold/node-a"),
            SingleComputerRuntime("node-b", root="/var/lib/manyfold/node-b"),
            SingleComputerRuntime("node-c", root="/var/lib/manyfold/node-c"),
        )
    )
    shifts = cluster.shift_many(_kafka_topology_shift_specs())
    kafka, _, _, client, _, _ = _compose_kafka_from_shifts(shifts)
    client.produce(b"before-failure")
    initial_leader = _require_leader(kafka).name

    kafka.kill_broker(initial_leader)
    recovered_snapshot = kafka.leader_election.snapshot()
    recovered_replication = kafka.replication_health.snapshot()
    continued_offsets = tuple(
        client.produce(f"after-failure-{index}".encode("ascii"))
        for index in range(3)
    )

    second_broker = _require_leader(kafka).name
    kafka.kill_broker(second_broker)
    lost_quorum_snapshot = kafka.leader_election.snapshot()
    lost_quorum_replication = kafka.replication_health.snapshot()
    try:
        client.produce(b"after-quorum-loss")
    except RuntimeError as error:
        quorum_lost_error = str(error)
    else:
        raise RuntimeError("Kafka accepted write after quorum loss")

    return {
        "initial_leader": initial_leader,
        "killed_leader": initial_leader,
        "re_elected_leader": recovered_snapshot.leader,
        "live_after_leader_kill": recovered_snapshot.live_nodes,
        "in_sync_after_leader_kill": recovered_replication.in_sync_nodes,
        "required_acks": recovered_replication.required_acks,
        "degraded_after_leader_kill": recovered_snapshot.is_degraded,
        "continued_offsets": continued_offsets,
        "request_count_after_recovery": kafka.request_count,
        "killed_second_broker": second_broker,
        "leader_after_quorum_loss": lost_quorum_snapshot.leader,
        "live_after_quorum_loss": lost_quorum_snapshot.live_nodes,
        "in_sync_after_quorum_loss": lost_quorum_replication.in_sync_nodes,
        "replication_can_write_after_quorum_loss": (
            lost_quorum_replication.can_accept_writes
        ),
        "quorum_lost_error": quorum_lost_error,
    }


def run_kafka_bootstrap_benchmark(
    requests: int = 1_000,
    *,
    mode: KafkaBenchmarkMode = KafkaBenchmarkMode.Full,
) -> KafkaDormantBootstrapBenchmarkResult:
    _require_positive_int(requests, "requests")
    if not isinstance(mode, KafkaBenchmarkMode):
        raise TypeError("Kafka benchmark mode must be a KafkaBenchmarkMode")
    cluster = RuntimeCluster(
        (
            SingleComputerRuntime("node-a", root="/var/lib/manyfold/node-a"),
            SingleComputerRuntime("node-b", root="/var/lib/manyfold/node-b"),
            SingleComputerRuntime("node-c", root="/var/lib/manyfold/node-c"),
        )
    )
    cluster.observe("node-a", ResourceObservation(cpu=0.20, memory=0.20))
    cluster.observe("node-b", ResourceObservation(cpu=0.20, memory=0.20))
    cluster.observe("node-c", ResourceObservation(cpu=0.20, memory=0.20))
    graph_routing = mode in (KafkaBenchmarkMode.Graph, KafkaBenchmarkMode.Full)
    durable = mode in (KafkaBenchmarkMode.Disk, KafkaBenchmarkMode.Full)
    tcp = mode in (KafkaBenchmarkMode.Socket, KafkaBenchmarkMode.Full)

    with tempfile.TemporaryDirectory(prefix="manyfold-kafka-benchmark-") as root:
        started_at = perf_counter()
        shifts = cluster.shift_many(_kafka_topology_shift_specs())
        kafka = ShiftedKafkaCluster.from_shifts(
            _broker_shifts(shifts),
            graph_routing=graph_routing,
            durable_root=Path(root) if durable else None,
        )
        discovery = ServiceDiscoveryBinding(
            _require_one_capability(
                tuple(shift.handle for shift in shifts),
                "service.discovery",
            )
        )
        ingress = LoadBalancerBinding(
            _require_one_capability(
                tuple(shift.handle for shift in shifts),
                "ingress.load_balance",
            )
        )
        producer = KafkaProducerBinding(
            _require_one_capability(
                tuple(shift.handle for shift in shifts),
                "kafka.producer",
            )
        )
        consumer = KafkaConsumerBinding(
            _require_one_capability(
                tuple(shift.handle for shift in shifts),
                "kafka.consumer",
            )
        )
        topology = NetworkTopologyBinding(
            _require_one_capability(
                tuple(shift.handle for shift in shifts),
                "network.topology",
            )
        )
        topology.configure_kafka(
            service="kafka",
            discovery=discovery,
            ingress=ingress,
            kafka=kafka,
            producer=producer,
            consumer=consumer,
        )
        boot_seconds = perf_counter() - started_at

        request_started_at = perf_counter()
        try:
            if tcp:
                produce_stream = KafkaProduceStreamBinding(ingress)
                with TcpStreamServer(
                    name="kafka-produce",
                    handler=produce_stream.input.tcp_request.receive,
                ) as server:
                    tcp_client = server.client()
                    for index in range(requests):
                        produce_stream.produce(
                            tcp_client,
                            f"request-{index}".encode(),
                        )
            else:
                for index in range(requests):
                    producer.produce(f"request-{index}".encode())
            kafka.require_fully_replicated()
            request_seconds = perf_counter() - request_started_at
        finally:
            kafka.close()

    return {
        "mode": mode.value,
        "requests": requests,
        "shifted_processes": len(shifts),
        "broker_count": len(kafka.brokers),
        "leader": _require_leader(kafka).name,
        "boot_seconds": boot_seconds,
        "request_seconds": request_seconds,
        "total_seconds": boot_seconds + request_seconds,
        "average_request_us": request_seconds * 1_000_000 / requests,
        "dormant_processes_remaining": cluster.dormant_process_count(),
    }


def run_kafka_backed_queue_program(
    *,
    messages: int,
    interval_seconds: float = 5.0,
    output: Callable[[str], None] = print,
) -> KafkaQueueProgramResult:
    _require_positive_int(messages, "messages")
    if isinstance(interval_seconds, bool) or not isinstance(interval_seconds, int | float):
        raise TypeError("queue interval_seconds must be a number")
    if interval_seconds < 0.0:
        raise ValueError("queue interval_seconds must be non-negative")
    if not callable(output):
        raise TypeError("queue output must be callable")
    cluster = RuntimeCluster(
        (
            SingleComputerRuntime("node-a", root="/var/lib/manyfold/node-a"),
            SingleComputerRuntime("node-b", root="/var/lib/manyfold/node-b"),
            SingleComputerRuntime("node-c", root="/var/lib/manyfold/node-c"),
        )
    )
    client = ManyFoldKafkaQueueClient(cluster)
    acks: list[str] = []
    try:
        for index in range(messages):
            if index:
                sleep(float(interval_seconds))
            payload = f"manyfold-queue-message-{index}".encode("ascii")
            ack = client.send(payload).display()
            output(ack)
            acks.append(ack)
    finally:
        client.close()
    return {
        "messages": messages,
        "interval_seconds": float(interval_seconds),
        "acks": tuple(acks),
        "shifted_processes": client.shifted_processes,
    }


def run_pubsub_upgrade_example() -> PubSubUpgradeResult:
    """Run identical app code against ephemeral and durable PubSub connections."""

    topic = "orders.created"
    payload = b"order-123"

    subscriber = TopicSubscriberBinding(name="topic-consumer")
    pubsub = PubSub()
    pubsub.subscribe(topic, subscriber)
    pubsub_delivery = _publish_pubsub_application_message(
        pubsub,
        topic=topic,
        payload=payload,
    )

    cluster = RuntimeCluster(
        (
            SingleComputerRuntime("node-a", root="/var/lib/manyfold/node-a"),
            SingleComputerRuntime("node-b", root="/var/lib/manyfold/node-b"),
            SingleComputerRuntime("node-c", root="/var/lib/manyfold/node-c"),
        )
    )
    durable_log = ReplicatedDurableLog(queue=ManyFoldKafkaQueueClient(cluster))
    durable = DurablePubSub(log=durable_log)
    try:
        durable_delivery = _publish_pubsub_application_message(
            durable,
            topic=topic,
            payload=payload,
        )
        durable_shifted_processes = durable.shifted_processes
    finally:
        durable.close()

    return {
        "topic": topic,
        "payload": payload.decode("ascii"),
        "pubsub_backing": pubsub_delivery.backing,
        "pubsub_delivered_to": pubsub_delivery.delivered_to,
        "pubsub_replayable": pubsub_delivery.replayable,
        "durable_backing": durable_delivery.backing,
        "durable_delivered_to": durable_delivery.delivered_to,
        "durable_replayable": durable_delivery.replayable,
        "durable_components": DurablePubSub.components,
        "durable_required_acks": durable_log.profile.required_acks,
        "durable_replica_count": durable_log.profile.replica_count,
        "durable_shifted_processes": durable_shifted_processes,
    }


def run_projection_into_local_stream_example() -> ProjectionIntoResult:
    """Project a stream record into a local client stream sink."""

    local_stream = LocalClientStreamSink(name="local-client")
    projection = StreamingProjection(
        name="order_summary",
        output_topic="client.order_summary",
        transform=lambda message: b"summary:" + message.payload,
    )
    delivery = projection.into(local_stream).consume(
        PubSubMessage(topic="orders.created", payload=b"order-123")
    )
    return {
        "projection": delivery.projection,
        "source_topic": delivery.source_topic,
        "output_topic": delivery.output_topic,
        "delivered_to": delivery.delivered_to,
        "payload": delivery.payload.decode("ascii"),
        "local_stream_messages": len(local_stream.messages),
    }


def _shift_summary(shift: ShiftResult) -> dict[str, str]:
    summary = {
        "runtime": shift.handle.runtime,
        "slot": str(shift.handle.slot),
        "name": shift.handle.name,
        "capability": shift.handle.capability,
        "address": shift.handle.address,
        "action": shift.action,
    }
    if shift.handle.proxy_from:
        summary["proxy_from"] = ",".join(shift.handle.proxy_from)
    return summary


def _compose_kafka_from_shifts(
    shifts: tuple[ShiftResult, ...],
) -> tuple[
    ShiftedKafkaCluster,
    ServiceDiscoveryBinding,
    LoadBalancerBinding,
    KafkaProducerBinding,
    KafkaConsumerBinding,
    NetworkTopologyBinding,
]:
    handles = tuple(shift.handle for shift in shifts)
    kafka = ShiftedKafkaCluster.from_shifts(_broker_shifts(shifts))
    discovery = ServiceDiscoveryBinding(
        _require_one_capability(handles, "service.discovery")
    )
    ingress = LoadBalancerBinding(
        _require_one_capability(handles, "ingress.load_balance")
    )
    producer = KafkaProducerBinding(
        _require_one_capability(handles, "kafka.producer")
    )
    consumer = KafkaConsumerBinding(
        _require_one_capability(handles, "kafka.consumer")
    )
    topology = NetworkTopologyBinding(
        _require_one_capability(handles, "network.topology")
    )
    topology.configure_kafka(
        service="kafka",
        discovery=discovery,
        ingress=ingress,
        kafka=kafka,
        producer=producer,
        consumer=consumer,
    )
    return kafka, discovery, ingress, producer, consumer, topology


def _kafka_topology_shift_specs() -> tuple[ShiftSpec, ...]:
    return (
        ShiftSpec(
            name="service-discovery",
            capability="service.discovery",
            data_dir=Path("service-discovery"),
        ),
        ShiftSpec(
            name="load-balancer",
            capability="ingress.load_balance",
            data_dir=Path("load-balancer"),
        ),
        ShiftSpec(
            name="network-topology",
            capability="network.topology",
            data_dir=Path("network/topology"),
        ),
        ShiftSpec(
            name="kafka-producer",
            capability="kafka.producer",
            data_dir=Path("producers/kafka"),
        ),
        ShiftSpec(
            name="kafka-consumer",
            capability="kafka.consumer",
            data_dir=Path("consumers/kafka"),
        ),
        ShiftSpec(
            name="kafka-broker-a",
            capability="kafka.broker",
            data_dir=Path("kafka/broker-a"),
        ),
        ShiftSpec(
            name="kafka-broker-b",
            capability="kafka.broker",
            data_dir=Path("kafka/broker-b"),
        ),
        ShiftSpec(
            name="kafka-broker-c",
            capability="kafka.broker",
            data_dir=Path("kafka/broker-c"),
        ),
    )


def _broker_shifts(shifts: tuple[ShiftResult, ...]) -> tuple[ShiftResult, ...]:
    return tuple(
        shift for shift in shifts if shift.handle.capability == "kafka.broker"
    )


def _discover_kafka_coordination(
    *,
    discovery: ServiceDiscoveryBinding,
    topology: NetworkTopologyBinding,
    service: str,
) -> KafkaCoordination:
    if not isinstance(discovery, ServiceDiscoveryBinding):
        raise TypeError("Kafka coordination discovery must be ServiceDiscoveryBinding")
    if not isinstance(topology, NetworkTopologyBinding):
        raise TypeError("Kafka coordination topology must be NetworkTopologyBinding")
    registration = discovery.require(service)
    configuration = topology.configuration_stream.latest()
    if not isinstance(configuration, NetworkTopologyConfiguration):
        raise RuntimeError("Kafka topology has not published configuration")
    if configuration.service != registration.service:
        raise RuntimeError(
            f"Kafka coordination service mismatch: "
            f"discovery={registration.service} topology={configuration.service}"
        )
    if configuration.endpoint != registration.endpoint:
        raise RuntimeError(
            f"Kafka coordination endpoint mismatch: "
            f"discovery={registration.endpoint} topology={configuration.endpoint}"
        )
    return KafkaCoordination(
        service=registration.service,
        endpoint=registration.endpoint,
        edges=configuration.edges,
    )


def _publish_pubsub_application_message(
    connection: PubSubConnection,
    *,
    topic: str,
    payload: bytes,
) -> PubSubDelivery:
    if not isinstance(connection, PubSubConnection):
        raise TypeError("pubsub application connection must be PubSubConnection")
    return connection.publish(PubSubMessage(topic=topic, payload=payload))


def _stream_name_from_topic(topic: str) -> str:
    topic = _require_non_empty_string(topic, "topic")
    safe = "".join(character if character.isalnum() else "_" for character in topic)
    return f"topic_{safe}"


def _require_capability(handle: ProcessHandle, capability: str) -> None:
    if not isinstance(handle, ProcessHandle):
        raise TypeError("capability check handle must be a ProcessHandle")
    _require_non_empty_string(capability, "capability")
    if handle.capability != capability:
        raise ValueError(
            f"process {handle.name} must provide {capability}, not {handle.capability}"
        )


def _require_one_capability(
    handles: tuple[ProcessHandle, ...],
    capability: str,
) -> ProcessHandle:
    matches = tuple(handle for handle in handles if handle.capability == capability)
    if len(matches) != 1:
        raise RuntimeError(
            f"expected one shifted process with capability {capability}, got {len(matches)}"
        )
    return matches[0]


def _require_leader(cluster: ShiftedKafkaCluster) -> ProcessHandle:
    leader = cluster.leader
    if leader is None:
        raise RuntimeError("Kafka cluster has no elected leader")
    return leader


def _require_node_set(nodes: tuple[str, ...], name: str) -> tuple[str, ...]:
    if not isinstance(nodes, tuple) or not nodes:
        raise ValueError(f"{name} must be a non-empty tuple")
    seen: set[str] = set()
    for node in nodes:
        _require_non_empty_string(node, name)
        if node in seen:
            raise ValueError(f"{name} must not contain duplicates: {node}")
        seen.add(node)
    return nodes


def _kafka_benchmark_graph_route():
    return route(
        owner="runtime_substrate",
        family="kafka_benchmark",
        stream="record",
        schema=Schema.bytes(name="RuntimeSubstrateKafkaRecord"),
    )


def _recv_exact(source, size: int) -> bytes:
    _require_non_negative_int(size, "receive size")
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = source.recv(remaining)
        if not chunk:
            raise RuntimeError("socket closed before receiving full Kafka frame")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _runtime_data_dir(root: Path, data_dir: Path) -> Path:
    if data_dir.is_absolute():
        return data_dir
    return root / data_dir


def _require_non_empty_string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _require_positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _require_non_negative_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _require_ratio(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"{name} must be a number")
    if value < 0.0 or value > 1.0:
        raise ValueError(f"{name} must be between 0.0 and 1.0")
    return float(value)


def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the ManyFold runtime substrate example."
    )
    parser.add_argument(
        "--kafka-benchmark",
        action="store_true",
        help="time shifting a Kafka-shaped cluster and receiving requests",
    )
    parser.add_argument(
        "--leader-failure",
        action="store_true",
        help="simulate killing the Kafka leader and continuing until quorum loss",
    )
    parser.add_argument(
        "--queue-program",
        action="store_true",
        help="run a small program backed by the ManyFold Kafka queue",
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=1_000,
        help="request count for --kafka-benchmark",
    )
    parser.add_argument(
        "--messages",
        type=int,
        default=3,
        help="message count for --queue-program",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=5.0,
        help="seconds between queue-program messages",
    )
    parser.add_argument(
        "--mode",
        choices=tuple(mode.value for mode in KafkaBenchmarkMode),
        default=KafkaBenchmarkMode.Full.value,
        help="benchmark path for --kafka-benchmark",
    )
    args = parser.parse_args()
    selected_modes = sum(
        (
            args.kafka_benchmark,
            args.leader_failure,
            args.queue_program,
        )
    )
    if selected_modes > 1:
        parser.error(
            "--kafka-benchmark, --leader-failure, and --queue-program are mutually exclusive"
        )
    if args.kafka_benchmark:
        result = run_kafka_bootstrap_benchmark(
            args.requests,
            mode=KafkaBenchmarkMode(args.mode),
        )
    elif args.leader_failure:
        result = run_kafka_leader_failure_example()
    elif args.queue_program:
        result = run_kafka_backed_queue_program(
            messages=args.messages,
            interval_seconds=args.interval_seconds,
        )
    else:
        result = run_example()
    print(json.dumps(result, indent=2, sort_keys=True))


class _TcpThreadingServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


class _TcpStreamHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        size_bytes = _recv_exact(self.request, 4)
        size = struct.unpack(">I", size_bytes)[0]
        payload = _recv_exact(self.request, size)
        response = self.server.stream_handler(payload)  # type: ignore[attr-defined]
        if not isinstance(response, bytes):
            raise TypeError("TCP stream handler must return bytes")
        self.request.sendall(struct.pack(">I", len(response)) + response)


if __name__ == "__main__":
    _main()
