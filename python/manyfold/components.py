"""Convenience components built from Manyfold graph primitives."""

from __future__ import annotations

import base64
import binascii
import json
import os
import tempfile
import threading
from dataclasses import dataclass
from json import JSONDecodeError
from pathlib import Path
from typing import Any, Generic, Iterator, TypeVar
from urllib.parse import quote, unquote

from ._manyfold_rust import Layer, Plane, Variant
from .graph import Graph
from .primitives import (
    OwnerName,
    Schema,
    StreamFamily,
    StreamName,
    SubscriptionLike,
    TypedEnvelope,
    TypedRoute,
    route,
)

T = TypeVar("T")
KeyPart = str | int
Key = tuple[str, ...]

Heartbeat = tuple[int, str]
RequestVote = tuple[int, str, int, int]
Vote = tuple[int, str, str, bool]
QuorumState = tuple[int, str, tuple[str, ...], bool]
AppendEntry = tuple[int, str]
ReplicatedLog = tuple[AppendEntry, ...]
LeaderState = tuple[str, int, bool]
_VoteLedger = dict[tuple[int, str], frozenset[str]]
_MEMORY_RECORD_FIELDS = (
    "route",
    "seq_source",
    "control_epoch",
    "schema_id",
    "schema_version",
    "payload_b64",
)
_VALUE_FILENAME = "__value__.bin"


@dataclass(frozen=True)
class StoreEntry:
    """One byte value stored under a keyspace prefix."""

    key: Key
    full_key: Key
    value: bytes


@dataclass(frozen=True)
class EventLogRecord(Generic[T]):
    """One typed event-log record loaded from durable bytes."""

    index: int
    value: T


@dataclass(frozen=True)
class EventLogRoutes(Generic[T]):
    """Typed append and committed ports for an event log."""

    append: TypedRoute[T]
    committed: TypedRoute[T]


@dataclass(frozen=True)
class SnapshotStoreRoutes(Generic[T]):
    """Typed write and latest ports for a snapshot store."""

    write: TypedRoute[T]
    latest: TypedRoute[T]


class FileStore:
    """Filesystem-backed byte store addressed by structured key prefixes."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def prefix(self, *parts: KeyPart) -> Keyspace:
        """Return a logical keyspace under ``parts``."""
        return Keyspace(self, _normalize_key(parts))

    def put(self, *parts: KeyPart, value: bytes) -> None:
        """Store bytes under a full key."""
        self.prefix().put(*parts, value=value)

    def get(self, *parts: KeyPart) -> bytes | None:
        """Return bytes for a full key, if present."""
        return self.prefix().get(*parts)

    def delete(self, *parts: KeyPart) -> bool:
        """Delete one full key if it exists."""
        return self.prefix().delete(*parts)

    def scan(self, *parts: KeyPart) -> tuple[StoreEntry, ...]:
        """Return all entries below a full-key prefix."""
        return self.prefix(*parts).scan()


@dataclass(frozen=True)
class Keyspace:
    """FoundationDB-style logical byte prefix over a ``FileStore``."""

    store: FileStore
    parts: Key = ()

    def __post_init__(self) -> None:
        if not isinstance(self.store, FileStore):
            raise ValueError("keyspace store must be a FileStore")
        if not isinstance(self.parts, tuple):
            raise ValueError("keyspace parts must be a tuple of strings or integers")
        object.__setattr__(self, "parts", _normalize_key(self.parts))

    def prefix(self, *parts: KeyPart) -> Keyspace:
        """Return a nested keyspace."""
        return Keyspace(self.store, (*self.parts, *_normalize_key(parts)))

    def key(self, *parts: KeyPart) -> Key:
        """Return a full key under this keyspace."""
        return (*self.parts, *_normalize_key(parts))

    def put(self, *parts: KeyPart, value: bytes) -> None:
        """Store bytes under a keyspace-relative key."""
        path = self._value_path(self.key(*parts))
        path.parent.mkdir(parents=True, exist_ok=True)
        _write_bytes_atomic(path, _normalize_bytes(value))

    def get(self, *parts: KeyPart) -> bytes | None:
        """Return bytes for a keyspace-relative key, if present."""
        path = self._value_path(self.key(*parts))
        if not path.is_file():
            return None
        return path.read_bytes()

    def delete(self, *parts: KeyPart) -> bool:
        """Delete one keyspace-relative key if it exists."""
        path = self._value_path(self.key(*parts))
        if not path.is_file():
            return False
        path.unlink()
        _prune_empty_directories(path.parent, self.store.root)
        return True

    def scan(self, *parts: KeyPart) -> tuple[StoreEntry, ...]:
        """Return all entries below a keyspace-relative prefix."""
        return tuple(
            StoreEntry(
                key=relative_key,
                full_key=full_key,
                value=path.read_bytes(),
            )
            for relative_key, full_key, path in self._iter_value_paths(self.key(*parts))
        )

    def keys(self, *parts: KeyPart) -> tuple[Key, ...]:
        """Return all relative keys below a prefix without reading stored bytes."""
        return tuple(
            relative_key
            for relative_key, _full_key, _path in self._iter_value_paths(
                self.key(*parts)
            )
        )

    def _iter_value_paths(self, full_prefix: Key) -> Iterator[tuple[Key, Key, Path]]:
        root = self._directory_path(full_prefix)
        if not root.exists():
            return
        entries: list[tuple[Key, Key, Path]] = []
        for path in root.rglob(_VALUE_FILENAME):
            full_key = self._key_from_value_path(path)
            if not _has_prefix(full_key, full_prefix):
                continue
            relative_key = full_key[len(self.parts) :]
            entries.append((relative_key, full_key, path))
        # Sort decoded keys once so callers see stable logical ordering rather than
        # filesystem-specific percent-encoded path ordering.
        yield from sorted(entries, key=lambda entry: entry[1])

    def _directory_path(self, key: Key) -> Path:
        path = self.store.root
        for part in key:
            path /= _encode_key_part(part)
        return path

    def _value_path(self, key: Key) -> Path:
        return self._directory_path(key) / _VALUE_FILENAME

    def _key_from_value_path(self, path: Path) -> Key:
        relative = path.relative_to(self.store.root)
        return tuple(_decode_key_part(part) for part in relative.parts[:-1])


class EventLog(Generic[T]):
    """Typed append-only log layered over a byte keyspace."""

    def __init__(
        self,
        name: str,
        keyspace: Keyspace,
        schema: Schema[T],
        *,
        owner: str | None = None,
    ) -> None:
        _require_component_name(name, "event log name")
        _require_keyspace(keyspace)
        _require_schema(schema)
        self.name = name
        self.keyspace = keyspace
        self.schema = schema
        self._append_lock = threading.RLock()
        self.routes = EventLogRoutes(
            append=_component_route(
                plane=Plane.Write,
                layer=Layer.Logical,
                owner=owner or name,
                family="event_log",
                stream="append",
                variant=Variant.Request,
                schema=schema,
            ),
            committed=_component_route(
                plane=Plane.Read,
                layer=Layer.Logical,
                owner=owner or name,
                family="event_log",
                stream="committed",
                variant=Variant.Meta,
                schema=schema,
            ),
        )

    def input(self, name: str = "append") -> TypedRoute[T]:
        if name != "append":
            raise KeyError(f"unknown event log input: {name}")
        return self.routes.append

    def output(self, name: str = "committed") -> TypedRoute[T]:
        if name != "committed":
            raise KeyError(f"unknown event log output: {name}")
        return self.routes.committed

    def install(self, graph: Graph) -> SubscriptionLike:
        """Append input values to the log, then publish committed values."""

        def on_next(envelope: TypedEnvelope[T]) -> None:
            with self._append_lock:
                index = self._next_index()
                self.keyspace.put(
                    _format_log_index(index),
                    value=self.schema.encode(envelope.value),
                )
                graph.publish(
                    self.routes.committed,
                    envelope.value,
                    control_epoch=envelope.closed.control_epoch,
                )

        return graph.observe(self.routes.append, replay_latest=False).subscribe(on_next)

    def records(self) -> tuple[EventLogRecord[T], ...]:
        """Return all durable log records in index order."""
        records: list[EventLogRecord[T]] = []
        for entry in self.keyspace.scan():
            if len(entry.key) != 1:
                continue
            index = _parse_log_index(entry.key[0])
            if index is None:
                continue
            records.append(
                EventLogRecord(
                    index=index,
                    value=self.schema.decode(entry.value),
                )
            )
        return tuple(sorted(records, key=lambda record: record.index))

    def replay(self, graph: Graph) -> tuple[EventLogRecord[T], ...]:
        """Publish durable records to the committed output route."""
        records = self.records()
        for record in records:
            graph.publish(self.routes.committed, record.value)
        return records

    def _next_index(self) -> int:
        indexes = (
            index
            for key in self.keyspace.keys()
            if len(key) == 1
            for index in (_parse_log_index(key[0]),)
            if index is not None
        )
        return max(indexes, default=0) + 1


class SnapshotStore(Generic[T]):
    """Typed latest-value store layered over a byte keyspace."""

    def __init__(
        self,
        name: str,
        keyspace: Keyspace,
        schema: Schema[T],
        *,
        owner: str | None = None,
        key: KeyPart = "latest",
    ) -> None:
        _require_component_name(name, "snapshot store name")
        _require_keyspace(keyspace)
        _require_schema(schema)
        self.name = name
        self.keyspace = keyspace
        self.schema = schema
        self.key = _normalize_key_part(key)
        self._write_lock = threading.RLock()
        self.routes = SnapshotStoreRoutes(
            write=_component_route(
                plane=Plane.Write,
                layer=Layer.Logical,
                owner=owner or name,
                family="snapshot_store",
                stream="write",
                variant=Variant.Request,
                schema=schema,
            ),
            latest=_component_route(
                plane=Plane.State,
                layer=Layer.Logical,
                owner=owner or name,
                family="snapshot_store",
                stream="latest",
                variant=Variant.State,
                schema=schema,
            ),
        )

    def input(self, name: str = "write") -> TypedRoute[T]:
        if name != "write":
            raise KeyError(f"unknown snapshot store input: {name}")
        return self.routes.write

    def output(self, name: str = "latest") -> TypedRoute[T]:
        if name != "latest":
            raise KeyError(f"unknown snapshot store output: {name}")
        return self.routes.latest

    def install(self, graph: Graph) -> SubscriptionLike:
        """Store input values, then publish the latest output."""

        def on_next(envelope: TypedEnvelope[T]) -> None:
            with self._write_lock:
                self.write(envelope.value)
                graph.publish(
                    self.routes.latest,
                    envelope.value,
                    control_epoch=envelope.closed.control_epoch,
                )

        return graph.observe(self.routes.write, replay_latest=False).subscribe(on_next)

    def write(self, value: T) -> None:
        """Store one latest value without publishing to a graph."""
        with self._write_lock:
            self.keyspace.put(self.key, value=self.schema.encode(value))

    def latest(self) -> T | None:
        """Return the durable latest value, if present."""
        with self._write_lock:
            payload = self.keyspace.get(self.key)
            if payload is None:
                return None
            return self.schema.decode(payload)

    def publish_latest(self, graph: Graph) -> T | None:
        """Publish the durable latest value to the latest output route."""
        with self._write_lock:
            payload = self.keyspace.get(self.key)
            if payload is None:
                return None
            value = self.schema.decode(payload)
            graph.publish(self.routes.latest, value)
            return value


@dataclass(frozen=True)
class ConsensusRoutes:
    """Typed route bundle used by the default consensus component."""

    election_tick: TypedRoute[bytes]
    election_timeout: TypedRoute[bytes]
    request_vote: TypedRoute[RequestVote]
    heartbeat: TypedRoute[Heartbeat]
    heartbeat_seen: TypedRoute[Heartbeat]
    vote_response: TypedRoute[Vote]
    proposed_entries: TypedRoute[AppendEntry]
    append_entries: TypedRoute[AppendEntry]
    quorum: TypedRoute[QuorumState]
    replicated_log: TypedRoute[ReplicatedLog]
    leader_state: TypedRoute[LeaderState]


@dataclass(frozen=True)
class MemoryRecord(Generic[T]):
    """One persisted route value decoded from disk."""

    route_display: str
    value: T
    seq_source: int
    control_epoch: int | None


class Memory:
    """Disk-backed route memory that can record and resume typed values."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._append_lock = threading.Lock()
        self._resuming: set[tuple[str, str, int | None]] = set()
        self._seen: set[tuple[str, int, str, int | None]] = set(
            self._event_key(record) for record in self._iter_raw_records()
        )

    def remember(
        self,
        graph: Graph,
        route_ref: TypedRoute[T],
        *,
        replay_latest: bool = True,
    ) -> SubscriptionLike:
        """Append future values for ``route_ref`` to disk."""

        def on_next(envelope: TypedEnvelope[T]) -> None:
            closed = envelope.closed
            with self._append_lock:
                payload_b64 = base64.b64encode(
                    route_ref.schema.encode(envelope.value)
                ).decode("ascii")
                event_key = (
                    closed.route.display(),
                    closed.seq_source,
                    payload_b64,
                    closed.control_epoch,
                )
                resume_key = (
                    closed.route.display(),
                    payload_b64,
                    closed.control_epoch,
                )
                if resume_key in self._resuming:
                    return
                if event_key in self._seen:
                    return
                self._seen.add(event_key)
                record = {
                    "route": closed.route.display(),
                    "seq_source": closed.seq_source,
                    "control_epoch": closed.control_epoch,
                    "schema_id": route_ref.schema.schema_id,
                    "schema_version": route_ref.schema.version,
                    "payload_b64": payload_b64,
                }
                with self.path.open("a", encoding="utf-8") as handle:
                    handle.write(
                        json.dumps(record, separators=(",", ":"), sort_keys=True)
                    )
                    handle.write("\n")

        return graph.observe(route_ref, replay_latest=replay_latest).subscribe(on_next)

    def records(self, route_ref: TypedRoute[T]) -> tuple[MemoryRecord[T], ...]:
        """Return decoded records for one typed route without publishing them."""
        return tuple(self._iter_records(route_ref))

    def resume(
        self, graph: Graph, route_ref: TypedRoute[T]
    ) -> tuple[MemoryRecord[T], ...]:
        """Publish remembered values for ``route_ref`` into ``graph``."""
        records = self.records(route_ref)
        for record in records:
            payload_b64 = base64.b64encode(
                route_ref.schema.encode(record.value)
            ).decode("ascii")
            resume_key = (
                route_ref.display(),
                payload_b64,
                record.control_epoch,
            )
            with self._append_lock:
                self._resuming.add(resume_key)
            try:
                envelope = graph.publish(
                    route_ref,
                    record.value,
                    control_epoch=record.control_epoch,
                )
                with self._append_lock:
                    self._seen.add(
                        (
                            envelope.closed.route.display(),
                            envelope.closed.seq_source,
                            payload_b64,
                            envelope.closed.control_epoch,
                        )
                    )
            finally:
                with self._append_lock:
                    self._resuming.discard(resume_key)
        return records

    def _iter_records(self, route_ref: TypedRoute[T]) -> Iterator[MemoryRecord[T]]:
        route_display = route_ref.display()
        for record in self._iter_raw_records():
            if record.get("route") != route_display:
                continue
            self._validate_record_schema(record, route_ref)
            payload = base64.b64decode(record["payload_b64"])
            yield MemoryRecord(
                route_display=route_display,
                value=route_ref.schema.decode(payload),
                seq_source=int(record["seq_source"]),
                control_epoch=record.get("control_epoch"),
            )

    def _iter_raw_records(self) -> Iterator[dict[str, Any]]:
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    record = json.loads(stripped)
                except JSONDecodeError as exc:
                    raise ValueError(
                        f"memory file {self.path} line {line_number} is not valid JSON"
                    ) from exc
                if not isinstance(record, dict):
                    raise ValueError(
                        f"memory file {self.path} line {line_number} must be a JSON object"
                    )
                for field in _MEMORY_RECORD_FIELDS:
                    if field not in record:
                        raise ValueError(
                            f"memory file {self.path} line {line_number} is missing {field}"
                        )
                self._validate_raw_record(record, line_number)
                yield record

    def _validate_raw_record(self, record: dict[str, Any], line_number: int) -> None:
        for field in ("route", "schema_id", "payload_b64"):
            if not isinstance(record[field], str):
                raise ValueError(
                    f"memory file {self.path} line {line_number} field {field} "
                    "must be a string"
                )
        for field in ("route", "schema_id"):
            if not record[field].strip():
                raise ValueError(
                    f"memory file {self.path} line {line_number} field {field} "
                    "must be a non-empty string"
                )
        for field in ("seq_source", "schema_version"):
            if not _is_plain_int(record[field]):
                raise ValueError(
                    f"memory file {self.path} line {line_number} field {field} "
                    "must be an integer"
                )
        if record["seq_source"] <= 0:
            raise ValueError(
                f"memory file {self.path} line {line_number} field seq_source "
                "must be positive"
            )
        if record["schema_version"] <= 0:
            raise ValueError(
                f"memory file {self.path} line {line_number} field schema_version "
                "must be positive"
            )
        if record["control_epoch"] is not None and not _is_plain_int(
            record["control_epoch"]
        ):
            raise ValueError(
                f"memory file {self.path} line {line_number} field control_epoch "
                "must be an integer or null"
            )
        if record["control_epoch"] is not None and record["control_epoch"] < 0:
            raise ValueError(
                f"memory file {self.path} line {line_number} field control_epoch "
                "must be non-negative"
            )
        try:
            base64.b64decode(record["payload_b64"], validate=True)
        except binascii.Error as exc:
            raise ValueError(
                f"memory file {self.path} line {line_number} field payload_b64 "
                "must be valid base64"
            ) from exc

    def _validate_record_schema(
        self,
        record: dict[str, Any],
        route_ref: TypedRoute[Any],
    ) -> None:
        if (
            record["schema_id"] == route_ref.schema.schema_id
            and record["schema_version"] == route_ref.schema.version
        ):
            return
        raise ValueError(
            f"memory file {self.path} has schema {record['schema_id']} "
            f"v{record['schema_version']} for route {route_ref.display()}, "
            f"expected {route_ref.schema.schema_id} v{route_ref.schema.version}"
        )

    @staticmethod
    def _event_key(record: dict[str, Any]) -> tuple[str, int, str, int | None]:
        return (
            str(record["route"]),
            int(record["seq_source"]),
            str(record["payload_b64"]),
            record.get("control_epoch"),
        )


class Consensus:
    """A default Raft-style leader-election component."""

    def __init__(
        self,
        graph: Graph,
        *,
        owner: str = "raft_cluster",
        nodes: tuple[str, ...] = ("node-a", "node-b", "node-c"),
        candidate_id: str = "node-a",
        term: int = 3,
        election_timeout_ticks: int = 2,
    ) -> None:
        if not nodes:
            raise ValueError("nodes must contain at least one node")
        if not all(isinstance(node, str) and node.strip() for node in nodes):
            raise ValueError("nodes must contain non-empty string identifiers")
        if len(nodes) != len(set(nodes)):
            raise ValueError("nodes must contain unique node identifiers")
        if not isinstance(candidate_id, str) or not candidate_id.strip():
            raise ValueError("candidate_id must be a non-empty string")
        if candidate_id not in nodes:
            raise ValueError("candidate_id must be present in nodes")
        if not _is_plain_int(term) or term <= 0:
            raise ValueError("term must be a positive integer")
        if not _is_plain_int(election_timeout_ticks) or election_timeout_ticks <= 0:
            raise ValueError("election_timeout_ticks must be a positive integer")
        self.graph = graph
        self.owner = owner
        self.nodes = nodes
        self.candidate_id = candidate_id
        self.term = term
        self.quorum_size = len(nodes) // 2 + 1
        self.election_timeout_ticks = election_timeout_ticks
        self.routes = self.default_routes(owner)
        self._announced_quorums: set[tuple[int, str]] = set()
        self._installed = False

    @classmethod
    def install(
        cls,
        graph: Graph,
        **kwargs: Any,
    ) -> Consensus:
        """Build and install a consensus component on ``graph``."""
        component = cls(graph, **kwargs)
        component.install_wiring()
        return component

    @staticmethod
    def default_routes(owner: str = "raft_cluster") -> ConsensusRoutes:
        """Return the canonical route bundle for the default consensus graph."""
        return ConsensusRoutes(
            election_tick=_component_route(
                plane=Plane.Read,
                layer=Layer.Internal,
                owner=owner,
                family="raft",
                stream="election_tick",
                variant=Variant.Event,
                schema=Schema.bytes(name="RaftElectionTick"),
            ),
            election_timeout=_component_route(
                plane=Plane.Read,
                layer=Layer.Internal,
                owner=owner,
                family="raft",
                stream="election_timeout",
                variant=Variant.Event,
                schema=Schema.bytes(name="RaftElectionTimeout"),
            ),
            request_vote=_component_route(
                plane=Plane.Write,
                layer=Layer.Logical,
                owner=owner,
                family="raft",
                stream="request_vote",
                variant=Variant.Request,
                schema=_request_vote_schema(),
            ),
            heartbeat=_component_route(
                plane=Plane.Read,
                layer=Layer.Logical,
                owner=owner,
                family="raft",
                stream="heartbeat",
                variant=Variant.Meta,
                schema=_heartbeat_schema(),
            ),
            heartbeat_seen=_component_route(
                plane=Plane.State,
                layer=Layer.Logical,
                owner=owner,
                family="raft",
                stream="heartbeat_seen",
                variant=Variant.State,
                schema=_heartbeat_schema(),
            ),
            vote_response=_component_route(
                plane=Plane.Read,
                layer=Layer.Logical,
                owner=owner,
                family="raft",
                stream="vote_response",
                variant=Variant.Meta,
                schema=_vote_schema(),
            ),
            proposed_entries=_component_route(
                plane=Plane.Write,
                layer=Layer.Logical,
                owner=owner,
                family="raft",
                stream="proposed_entries",
                variant=Variant.Request,
                schema=_append_entry_schema(),
            ),
            append_entries=_component_route(
                plane=Plane.Write,
                layer=Layer.Logical,
                owner=owner,
                family="raft",
                stream="append_entries",
                variant=Variant.Request,
                schema=_append_entry_schema(),
            ),
            quorum=_component_route(
                plane=Plane.State,
                layer=Layer.Logical,
                owner=owner,
                family="raft",
                stream="quorum",
                variant=Variant.State,
                schema=_quorum_schema(),
            ),
            replicated_log=_component_route(
                plane=Plane.State,
                layer=Layer.Logical,
                owner=owner,
                family="raft",
                stream="replicated_log",
                variant=Variant.State,
                schema=_replicated_log_schema(),
            ),
            leader_state=_component_route(
                plane=Plane.State,
                layer=Layer.Logical,
                owner=owner,
                family="raft",
                stream="leader_state",
                variant=Variant.State,
                schema=_leader_state_schema(),
            ),
        )

    def install_wiring(self) -> None:
        """Install default capacitors, resistor, watchdog, and state transforms."""
        if self._installed:
            return
        routes = self.routes
        self.graph.capacitor(
            source=routes.heartbeat,
            sink=routes.heartbeat_seen,
            capacity=1,
            immediate=True,
        )
        self.graph.watchdog(
            reset_by=routes.heartbeat_seen,
            output=routes.election_timeout,
            after=self.election_timeout_ticks,
            clock=routes.election_tick,
        )
        self.graph.resistor(
            source=routes.proposed_entries,
            sink=routes.append_entries,
        )
        self.graph.stateful_map(
            routes.vote_response,
            initial_state={},
            step=self._accumulate_votes,
            output=routes.quorum,
        )
        self.graph.stateful_map(
            routes.append_entries,
            initial_state=(),
            step=lambda log, entry: (log + (entry,), log + (entry,)),
            output=routes.replicated_log,
        )
        self.graph.join_latest(
            routes.heartbeat_seen,
            routes.quorum,
            combine=lambda heartbeat, quorum: (
                heartbeat[1],
                heartbeat[0],
                quorum[3] and quorum[0] == heartbeat[0] and quorum[1] == heartbeat[1],
            ),
        ).subscribe(self._publish_confirmed_leader_state)
        self.graph.observe(routes.request_vote, replay_latest=False).subscribe(
            self._on_request_vote
        )
        self.graph.observe(routes.election_timeout, replay_latest=False).subscribe(
            lambda _item: self.start_election()
        )
        self.graph.observe(routes.quorum, replay_latest=False).subscribe(
            self._become_leader_on_quorum
        )
        self._installed = True

    def tick(self, control_epoch: int) -> None:
        """Advance the election clock by publishing one tick."""
        self.graph.publish(
            self.routes.election_tick,
            f"tick-{control_epoch}".encode("ascii"),
            control_epoch=control_epoch,
        )

    def start_election(self) -> None:
        """Publish the candidate self-vote and request votes from peers."""
        self.graph.publish(
            self.routes.vote_response,
            (self.term, self.candidate_id, self.candidate_id, True),
        )
        self.graph.publish(
            self.routes.request_vote,
            (self.term, self.candidate_id, 0, 0),
        )

    def propose(self, index: int, command: str) -> None:
        """Publish a proposed append entry into the consensus log."""
        self.graph.publish(self.routes.proposed_entries, (index, command))

    def latest_leader(self) -> LeaderState | None:
        latest = self.graph.latest(self.routes.leader_state)
        return None if latest is None else latest.value

    def latest_quorum(self) -> QuorumState | None:
        latest = self.graph.latest(self.routes.quorum)
        return None if latest is None else latest.value

    def latest_log(self) -> ReplicatedLog | None:
        latest = self.graph.latest(self.routes.replicated_log)
        return None if latest is None else latest.value

    def _accumulate_votes(
        self,
        ledger: _VoteLedger,
        vote_value: Vote,
    ) -> tuple[_VoteLedger, QuorumState]:
        term, candidate, voter, granted = vote_value
        election = (term, candidate)
        voters = ledger.get(election, frozenset())
        next_voters = voters | {voter} if granted else voters
        next_ledger = ledger
        if next_voters != voters:
            next_ledger = {**ledger, election: next_voters}
        stable_voters = tuple(sorted(next_voters))
        return (
            next_ledger,
            (term, candidate, stable_voters, len(stable_voters) >= self.quorum_size),
        )

    def _on_request_vote(self, request: TypedEnvelope[RequestVote]) -> None:
        term, candidate, last_log_index, last_log_term = request.value
        grant = term == self.term and candidate == self.candidate_id
        grant = grant and last_log_index >= 0 and last_log_term >= 0
        for node in self.nodes:
            if node != candidate:
                self.graph.publish(
                    self.routes.vote_response,
                    (term, candidate, node, grant),
                )

    def _become_leader_on_quorum(self, quorum: TypedEnvelope[QuorumState]) -> None:
        term, candidate, _voters, has_quorum = quorum.value
        election = (term, candidate)
        is_self_election = term == self.term and candidate == self.candidate_id
        if has_quorum and is_self_election and election not in self._announced_quorums:
            self._announced_quorums.add(election)
            self.graph.publish(self.routes.heartbeat, (term, candidate))

    def _publish_confirmed_leader_state(self, state: LeaderState) -> None:
        if state[2]:
            self.graph.publish(self.routes.leader_state, state)


def _component_route(
    *,
    plane: Plane,
    layer: Layer,
    owner: str,
    family: str,
    stream: str,
    variant: Variant,
    schema: Schema[T],
) -> TypedRoute[T]:
    return route(
        plane=plane,
        layer=layer,
        owner=OwnerName(owner),
        family=StreamFamily(family),
        stream=StreamName(stream),
        variant=variant,
        schema=schema,
    )


def _require_component_name(value: object, field: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")


def _require_keyspace(value: object) -> None:
    if not isinstance(value, Keyspace):
        raise ValueError("keyspace must be a Keyspace")


def _require_schema(value: object) -> None:
    if not isinstance(value, Schema):
        raise ValueError("schema must be a Schema")


def _normalize_key(parts: tuple[KeyPart, ...]) -> Key:
    return tuple(_normalize_key_part(part) for part in parts)


def _normalize_key_part(part: KeyPart) -> str:
    if isinstance(part, str):
        key = part
    elif _is_plain_int(part):
        key = str(part)
    else:
        raise ValueError("key parts must be strings or integers")
    if "\x00" in key:
        raise ValueError("key parts cannot contain NUL bytes")
    return key


def _normalize_bytes(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray | memoryview):
        return bytes(value)
    raise ValueError("stored values must be bytes-like")


def _encode_key_part(part: str) -> str:
    encoded = quote(part, safe="")
    if encoded == "":
        return "%00"
    if encoded == ".":
        return "%2E"
    if encoded == "..":
        return "%2E%2E"
    if encoded == _VALUE_FILENAME:
        return "%5F%5Fvalue%5F%5F%2Ebin"
    return encoded


def _decode_key_part(part: str) -> str:
    if part == "%00":
        return ""
    return unquote(part)


def _has_prefix(key: Key, prefix: Key) -> bool:
    return key[: len(prefix)] == prefix


def _is_plain_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _format_log_index(index: int) -> str:
    return f"{index:020d}"


def _parse_log_index(value: str) -> int | None:
    if len(value) != 20 or not value.isdigit():
        return None
    index = int(value)
    if index <= 0 or _format_log_index(index) != value:
        return None
    return index


def _write_bytes_atomic(path: Path, value: bytes) -> None:
    with tempfile.NamedTemporaryFile(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temporary_path = Path(handle.name)
        try:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException:
            temporary_path.unlink(missing_ok=True)
            raise
    try:
        os.replace(temporary_path, path)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise


def _prune_empty_directories(path: Path, stop: Path) -> None:
    current = path
    while current != stop:
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent


def _heartbeat_schema() -> Schema[Heartbeat]:
    return Schema(
        schema_id="RaftHeartbeat",
        version=1,
        encode=_encode_heartbeat,
        decode=lambda payload: _decode_heartbeat(payload),
    )


def _encode_heartbeat(value: Heartbeat) -> bytes:
    term, leader = _encode_tuple_fields(value, 2, "heartbeat")
    return _encode_json_tuple(
        (_encode_int(term, "term"), _encode_string(leader, "leader"))
    )


def _decode_heartbeat(payload: bytes) -> Heartbeat:
    text = payload.decode("utf-8")
    if _is_json_tuple_payload(text):
        term, leader = _decode_json_tuple(text, 2, "heartbeat")
        return (_decode_json_int(term), _decode_json_string(leader, "leader"))
    term_text, leader = text.split("|", 1)
    return (int(term_text), leader)


def _request_vote_schema() -> Schema[RequestVote]:
    return Schema(
        schema_id="RaftRequestVote",
        version=1,
        encode=_encode_request_vote,
        decode=lambda payload: _decode_request_vote(payload),
    )


def _encode_request_vote(value: RequestVote) -> bytes:
    term, candidate, last_log_index, last_log_term = _encode_tuple_fields(
        value,
        4,
        "request vote",
    )
    return _encode_json_tuple(
        (
            _encode_int(term, "term"),
            _encode_string(candidate, "candidate"),
            _encode_int(last_log_index, "last_log_index"),
            _encode_int(last_log_term, "last_log_term"),
        )
    )


def _decode_request_vote(payload: bytes) -> RequestVote:
    text = payload.decode("utf-8")
    if _is_json_tuple_payload(text):
        term, candidate, last_log_index, last_log_term = _decode_json_tuple(
            text,
            4,
            "request vote",
        )
        return (
            _decode_json_int(term),
            _decode_json_string(candidate, "candidate"),
            _decode_json_int(last_log_index),
            _decode_json_int(last_log_term),
        )
    term_text, candidate, index_text, term_at_index_text = text.split("|", 3)
    return (int(term_text), candidate, int(index_text), int(term_at_index_text))


def _vote_schema() -> Schema[Vote]:
    return Schema(
        schema_id="RaftVote",
        version=1,
        encode=_encode_vote,
        decode=lambda payload: _decode_vote(payload),
    )


def _encode_vote(value: Vote) -> bytes:
    term, candidate, voter, granted = _encode_tuple_fields(value, 4, "vote")
    return _encode_json_tuple(
        (
            _encode_int(term, "term"),
            _encode_string(candidate, "candidate"),
            _encode_string(voter, "voter"),
            _encode_bool(granted, "granted"),
        )
    )


def _decode_vote(payload: bytes) -> Vote:
    text = payload.decode("utf-8")
    if _is_json_tuple_payload(text):
        term, candidate, voter, granted = _decode_json_tuple(text, 4, "vote")
        return (
            _decode_json_int(term),
            _decode_json_string(candidate, "candidate"),
            _decode_json_string(voter, "voter"),
            _decode_json_bool(granted),
        )
    term_text, candidate, voter, granted_text = text.split("|", 3)
    return (int(term_text), candidate, voter, _decode_legacy_bool(granted_text))


def _quorum_schema() -> Schema[QuorumState]:
    return Schema(
        schema_id="RaftQuorumState",
        version=1,
        encode=_encode_quorum,
        decode=lambda payload: _decode_quorum(payload),
    )


def _encode_quorum(value: QuorumState) -> bytes:
    term, candidate, voters, granted = _encode_tuple_fields(value, 4, "quorum")
    return _encode_json_tuple(
        (
            _encode_int(term, "term"),
            _encode_string(candidate, "candidate"),
            _encode_string_sequence(voters, "quorum voters"),
            _encode_bool(granted, "granted"),
        )
    )


def _decode_quorum(payload: bytes) -> QuorumState:
    text = payload.decode("utf-8")
    if _is_json_tuple_payload(text):
        term, candidate, voters, granted = _decode_json_tuple(text, 4, "quorum")
        return (
            _decode_json_int(term),
            _decode_json_string(candidate, "candidate"),
            _decode_json_string_array(voters, "quorum voters"),
            _decode_json_bool(granted),
        )
    term_text, candidate, voters_text, granted_text = text.split("|", 3)
    voters = tuple(voter for voter in voters_text.split(",") if voter)
    return (int(term_text), candidate, voters, _decode_legacy_bool(granted_text))


def _append_entry_schema() -> Schema[AppendEntry]:
    return Schema(
        schema_id="RaftAppendEntry",
        version=1,
        encode=_encode_append_entry,
        decode=lambda payload: _decode_append_entry(payload),
    )


def _encode_append_entry(value: AppendEntry) -> bytes:
    return _encode_json_tuple(_encode_append_entry_value(value))


def _decode_append_entry(payload: bytes) -> AppendEntry:
    text = payload.decode("utf-8")
    if _is_json_tuple_payload(text):
        index, command = _decode_json_tuple(text, 2, "append entry")
        return (_decode_json_int(index), _decode_json_string(command, "command"))
    index_text, command = text.split("|", 1)
    return (int(index_text), command)


def _replicated_log_schema() -> Schema[ReplicatedLog]:
    return Schema(
        schema_id="RaftReplicatedLog",
        version=1,
        encode=_encode_replicated_log,
        decode=lambda payload: _decode_replicated_log(payload),
    )


def _encode_replicated_log(value: ReplicatedLog) -> bytes:
    if not isinstance(value, (tuple, list)):
        raise ValueError("replicated log must be a tuple of append entries")
    entries = tuple(_encode_append_entry_value(entry) for entry in value)
    return json.dumps(entries, allow_nan=False, separators=(",", ":")).encode("utf-8")


def _decode_replicated_log(payload: bytes) -> ReplicatedLog:
    text = payload.decode("utf-8")
    if not text:
        return ()
    if _is_json_tuple_payload(text):
        entries = json.loads(text.lstrip())
        return tuple(_decode_json_append_entry(entry) for entry in entries)
    return tuple(
        _decode_append_entry(line.encode("utf-8")) for line in text.splitlines()
    )


def _leader_state_schema() -> Schema[LeaderState]:
    return Schema(
        schema_id="RaftLeaderState",
        version=1,
        encode=_encode_leader_state,
        decode=lambda payload: _decode_leader_state(payload),
    )


def _encode_leader_state(value: LeaderState) -> bytes:
    leader, term, committed = _encode_tuple_fields(value, 3, "leader state")
    return _encode_json_tuple(
        (
            _encode_string(leader, "leader"),
            _encode_int(term, "term"),
            _encode_bool(committed, "committed"),
        )
    )


def _decode_leader_state(payload: bytes) -> LeaderState:
    text = payload.decode("utf-8")
    if _is_json_tuple_payload(text):
        leader, term, committed = _decode_json_tuple(text, 3, "leader state")
        return (
            _decode_json_string(leader, "leader"),
            _decode_json_int(term),
            _decode_json_bool(committed),
        )
    leader, term_text, committed_text = text.split("|", 2)
    return (leader, int(term_text), _decode_legacy_bool(committed_text))


def _decode_json_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    raise ValueError("JSON boolean field must be true or false")


def _decode_legacy_bool(value: str) -> bool:
    if value == "1":
        return True
    if value == "0":
        return False
    raise ValueError("legacy boolean field must be 0 or 1")


def _decode_json_int(value: Any) -> int:
    if _is_plain_int(value):
        return value
    raise ValueError("JSON integer field must be an integer")


def _decode_json_string(value: Any, field: str) -> str:
    if isinstance(value, str):
        return value
    raise ValueError(f"{field} must be a JSON string")


def _decode_json_string_array(value: Any, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError(f"{field} must be a JSON array")
    if not all(isinstance(item, str) for item in value):
        raise ValueError(f"{field} must contain only strings")
    return tuple(value)


def _decode_json_append_entry(value: Any) -> AppendEntry:
    if not isinstance(value, list) or len(value) != 2:
        raise ValueError("replicated log entries must be JSON [index, command] pairs")
    index, command = value
    return (_decode_json_int(index), _decode_json_string(command, "command"))


def _encode_append_entry_value(value: Any) -> AppendEntry:
    index, command = _encode_tuple_fields(value, 2, "append entry")
    return (_encode_int(index, "index"), _encode_string(command, "command"))


def _encode_tuple_fields(
    value: Any, expected_length: int, field: str
) -> tuple[Any, ...]:
    if not isinstance(value, (tuple, list)) or len(value) != expected_length:
        raise ValueError(f"{field} must be a tuple with {expected_length} fields")
    return tuple(value)


def _encode_bool(value: Any, field: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ValueError(f"{field} must be a boolean")


def _encode_int(value: Any, field: str) -> int:
    if _is_plain_int(value):
        return value
    raise ValueError(f"{field} must be an integer")


def _encode_string(value: Any, field: str) -> str:
    if isinstance(value, str):
        return value
    raise ValueError(f"{field} must be a string")


def _encode_string_sequence(value: Any, field: str) -> tuple[str, ...]:
    if not isinstance(value, (tuple, list)):
        raise ValueError(f"{field} must be a tuple of strings")
    if not all(isinstance(item, str) for item in value):
        raise ValueError(f"{field} must contain only strings")
    return tuple(value)


def _decode_json_tuple(text: str, expected_length: int, field: str) -> list[Any]:
    value = json.loads(text.lstrip())
    if not isinstance(value, list) or len(value) != expected_length:
        raise ValueError(f"{field} must be a JSON array with {expected_length} fields")
    return value


def _is_json_tuple_payload(text: str) -> bool:
    return text.lstrip().startswith("[")


def _encode_json_tuple(value: tuple[Any, ...]) -> bytes:
    return json.dumps(value, allow_nan=False, separators=(",", ":")).encode("utf-8")


__all__ = (
    "AppendEntry",
    "Consensus",
    "ConsensusRoutes",
    "EventLog",
    "EventLogRecord",
    "EventLogRoutes",
    "FileStore",
    "Heartbeat",
    "Keyspace",
    "LeaderState",
    "Memory",
    "MemoryRecord",
    "QuorumState",
    "ReplicatedLog",
    "RequestVote",
    "SnapshotStore",
    "SnapshotStoreRoutes",
    "StoreEntry",
    "Vote",
)
