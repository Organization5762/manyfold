"""Persistent Raft-backed coordination for ManyFold control-plane commands.

This module delegates the Raft protocol to PySyncObj. It deliberately does not
accept or connect to ManyFold PubSub streams; high-rate frame and sensor data
must remain on the data plane.
"""

from __future__ import annotations

import json
import math
import os
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, final

from pysyncobj import (
    FAIL_REASON,
    SyncObj,
    SyncObjConf,
    SyncObjConsumer,
    SyncObjException,
    replicated,
)

from .network import (
    NetworkProtocolConfig,
    RaftNetworkProtocol,
    resolve_network_protocol,
)

MAX_COMMAND_BYTES = 64 * 1024
MAX_COMMAND_KIND_LENGTH = 128
MAX_COMMAND_ID_LENGTH = 128
DEFAULT_COMMAND_TIMEOUT_SECONDS = 5.0
_RAFT_FOLLOWER_STATE = 0
_RAFT_CANDIDATE_STATE = 1
_RAFT_LEADER_STATE = 2
_RAFT_ROLE_BY_STATE = {
    _RAFT_FOLLOWER_STATE: "follower",
    _RAFT_CANDIDATE_STATE: "candidate",
    _RAFT_LEADER_STATE: "leader",
}
_NODE_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_COMMAND_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/-]*\Z")
_RAFT_JOURNAL_HEADER_BYTES = 40
_RAFT_JOURNAL_MAGIC = b"PYSYNCOBJ"
@final
@dataclass(frozen=True)
class MemberConfig:
    """One static coordinator member and its distinct Raft/API addresses."""

    node_id: str
    host: str
    raft_port: int
    api_port: int

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, str) or not _NODE_ID_PATTERN.fullmatch(
            self.node_id
        ):
            raise ValueError(
                "node_id must contain 1-64 letters, digits, dots, underscores, "
                "or hyphens and start with a letter or digit"
            )
        if not isinstance(self.host, str) or not self.host.strip():
            raise ValueError("member host must be a non-empty string")
        if ":" in self.host:
            raise ValueError("member host must be an IPv4 address or DNS name")
        _require_port(self.raft_port, "raft_port")
        _require_port(self.api_port, "api_port")
        if self.raft_port == self.api_port:
            raise ValueError("raft_port and api_port must be distinct")

    @property
    def raft_address(self) -> str:
        """Return the address PySyncObj uses as this member's Raft identity."""
        return f"{self.host}:{self.raft_port}"

    @property
    def raft_identity(self) -> str:
        """Return the explicit address-bound Raft identity."""
        return self.raft_address

    @property
    def api_address(self) -> str:
        """Return the coordinator HTTP address."""
        return f"{self.host}:{self.api_port}"

    @property
    def api_url(self) -> str:
        """Return the coordinator HTTP base URL."""
        return f"http://{self.api_address}"

    def to_dict(self) -> dict[str, object]:
        """Return the stable JSON representation."""
        return {
            "node_id": self.node_id,
            "host": self.host,
            "raft_port": self.raft_port,
            "raft_identity": self.raft_identity,
            "api_port": self.api_port,
        }


@final
@dataclass(frozen=True)
class ClusterConfig:
    """A fixed-membership coordinator configuration of any positive size."""

    members: tuple[MemberConfig, ...]
    network: NetworkProtocolConfig = field(default_factory=NetworkProtocolConfig)

    def __post_init__(self) -> None:
        if not isinstance(self.members, tuple) or not self.members:
            raise ValueError("a coordinator cluster must contain at least 1 member")
        if not all(isinstance(member, MemberConfig) for member in self.members):
            raise ValueError("cluster members must be MemberConfig values")
        if not isinstance(self.network, NetworkProtocolConfig):
            raise ValueError("cluster network must be a NetworkProtocolConfig")
        node_ids = {member.node_id for member in self.members}
        raft_identities = {member.raft_identity for member in self.members}
        api_addresses = {member.api_address for member in self.members}
        bound_addresses = {
            address
            for member in self.members
            for address in (member.raft_address, member.api_address)
        }
        if len(node_ids) != len(self.members):
            raise ValueError("coordinator node_id values must be unique")
        if len(raft_identities) != len(self.members):
            raise ValueError("coordinator Raft identities must be unique")
        if len(api_addresses) != len(self.members):
            raise ValueError("coordinator API addresses must be unique")
        if len(bound_addresses) != len(self.members) * 2:
            raise ValueError("coordinator Raft and API addresses must be distinct")

    def member(self, node_id: str) -> MemberConfig:
        """Return one member by stable application identity."""
        for member in self.members:
            if member.node_id == node_id:
                return member
        raise ValueError(f"unknown coordinator node_id {node_id!r}")

    def member_for_raft_address(self, address: str) -> MemberConfig | None:
        """Resolve a PySyncObj Raft identity to its configured member."""
        for member in self.members:
            if member.raft_address == address:
                return member
        return None

    def to_dict(self) -> dict[str, object]:
        """Return the stable JSON representation."""
        return {
            "members": [member.to_dict() for member in self.members],
            "network": self.network.to_dict(),
        }

    def save(self, path: str | Path) -> None:
        """Persist this configuration atomically."""
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        _write_json_atomic(destination, self.to_dict())

    @classmethod
    def load(cls, path: str | Path) -> ClusterConfig:
        """Load and validate a cluster configuration."""
        source = Path(path)
        try:
            value = json.loads(source.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ValueError(
                f"failed to load cluster config {source}: {error}"
            ) from error
        if not isinstance(value, dict) or not isinstance(value.get("members"), list):
            raise ValueError("cluster config must contain a members list")
        members = tuple(_member_from_json(member) for member in value["members"])
        network = NetworkProtocolConfig.from_json(value.get("network"))
        return cls(members, network)


@final
@dataclass(frozen=True)
class ControlCommand:
    """One bounded JSON control-plane mutation submitted by a client."""

    command_id: str
    kind: str
    payload: dict[str, object]

    def __post_init__(self) -> None:
        _require_command_token(
            self.command_id,
            "command_id",
            max_length=MAX_COMMAND_ID_LENGTH,
        )
        _require_command_token(
            self.kind,
            "kind",
            max_length=MAX_COMMAND_KIND_LENGTH,
        )
        if not isinstance(self.payload, dict):
            raise ValueError("command payload must be a JSON object")
        payload_json = _canonical_json(self.payload)
        size = len(payload_json.encode("utf-8"))
        if size > MAX_COMMAND_BYTES:
            raise ValueError(
                f"command payload is {size} bytes; maximum is {MAX_COMMAND_BYTES}"
            )

    @property
    def payload_json(self) -> str:
        """Return the canonical payload stored in the replicated log."""
        return _canonical_json(self.payload)

    @classmethod
    def from_json(cls, value: object) -> ControlCommand:
        """Validate an HTTP request object as a control-plane command."""
        if not isinstance(value, dict):
            raise ValueError("command request must be a JSON object")
        command_id = value.get("command_id")
        kind = value.get("kind")
        payload = value.get("payload")
        if not isinstance(command_id, str):
            raise ValueError("command_id must be a string")
        if not isinstance(kind, str):
            raise ValueError("kind must be a string")
        if not isinstance(payload, dict):
            raise ValueError("payload must be a JSON object")
        return cls(command_id=command_id, kind=kind, payload=payload)


@final
@dataclass(frozen=True)
class CommittedCommand:
    """One command after the Raft quorum has committed and applied it."""

    sequence: int
    command_id: str
    kind: str
    payload: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-compatible record."""
        return {
            "sequence": self.sequence,
            "command_id": self.command_id,
            "kind": self.kind,
            "payload": self.payload,
        }


@final
@dataclass(frozen=True)
class CoordinatorStatus:
    """A stable status view over PySyncObj's Raft metrics."""

    node_id: str
    raft_identity: str
    role: str
    leader: MemberConfig | None
    term: int
    raft_commit_index: int
    raft_last_applied: int
    control_log_sequence: int
    has_quorum: bool
    ready: bool

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-compatible status object."""
        return {
            "node_id": self.node_id,
            "raft_identity": self.raft_identity,
            "role": self.role,
            "leader": self.leader.to_dict() if self.leader is not None else None,
            "term": self.term,
            "raft_commit_index": self.raft_commit_index,
            "raft_last_applied": self.raft_last_applied,
            "control_log_sequence": self.control_log_sequence,
            "has_quorum": self.has_quorum,
            "ready": self.ready,
        }


@final
class NotLeaderError(RuntimeError):
    """Raised when a write reaches a follower or candidate."""

    def __init__(self, leader: MemberConfig | None) -> None:
        self.leader = leader
        detail = (
            f"; current leader is {leader.node_id!r}"
            if leader is not None
            else "; no leader is currently known"
        )
        super().__init__(f"coordinator is not the Raft leader{detail}")


@final
class CoordinatorUnavailableError(RuntimeError):
    """Raised when Raft cannot determine a committed write outcome."""


@final
class CorruptCoordinatorStateError(RuntimeError):
    """Raised when durable coordinator state fails integrity validation."""

    def __init__(self, path: Path, detail: str) -> None:
        self.path = path
        super().__init__(f"coordinator state is corrupt at {path}: {detail}")


class DurableWriteBoundary(Protocol):
    """Observe durable transaction stages for controlled fault injection."""

    def before_write(self, path: Path) -> None:
        """Run immediately before a durable write starts."""

    def before_commit(self, path: Path) -> None:
        """Run after mutation but before the durable transaction commits."""


@final
class PersistentRaftCoordinator:
    """A persistent, fixed-membership Raft coordinator for control commands."""

    def __init__(
        self,
        config: ClusterConfig,
        node_id: str,
        state_directory: str | Path,
        network_protocol: RaftNetworkProtocol | None = None,
        durable_write_boundary: DurableWriteBoundary | None = None,
    ) -> None:
        self.config = config
        self.member = config.member(node_id)
        self.state_directory = Path(state_directory).resolve()
        self.state_directory.mkdir(parents=True, exist_ok=True)
        _verify_state_identity(self.state_directory, config, self.member)
        _validate_raft_journal(self.state_directory / "raft.journal")

        self._database_path = self.state_directory / "committed.sqlite3"
        _initialize_database(self._database_path)
        self._control_log = _ControlPlaneLog(
            self._database_path,
            (
                durable_write_boundary
                if durable_write_boundary is not None
                else _DIRECT_DURABLE_WRITE
            ),
        )
        protocol = (
            network_protocol
            if network_protocol is not None
            else resolve_network_protocol(config.network)
        )
        raft_config = SyncObjConf(
            autoTick=True,
            appendEntriesBatchSizeBytes=64 * 1024,
            appendEntriesPeriod=0.08,
            appendEntriesUseBatch=False,
            bindAddress=self.member.raft_address,
            commandsQueueSize=128,
            commandsWaitLeader=False,
            connectionRetryTime=0.1,
            connectionTimeout=1.5,
            fullDumpFile=str(self.state_directory / "raft.snapshot"),
            journalFile=str(self.state_directory / "raft.journal"),
            leaderFallbackTimeout=0.8,
            logCompactionBatchSize=64 * 1024,
            logCompactionMinEntries=1024,
            logCompactionMinTime=60,
            maxBindRetries=1,
            raftMaxTimeout=0.7,
            raftMinTimeout=0.35,
            useFork=False,
        )
        peer_addresses = [
            member.raft_identity
            for member in config.members
            if member.node_id != node_id
        ]
        self._raft = SyncObj(
            self.member.raft_identity,
            peer_addresses,
            conf=raft_config,
            consumers=[self._control_log],
            nodeClass=protocol.node_class,
            transportClass=protocol.transport_factory(self.state_directory),
        )
        self._closed = False

    def status(self) -> CoordinatorStatus:
        """Return current election, quorum, and durable apply state."""
        raw_status = self._raft.getStatus()
        state = int(raw_status["state"])
        role = _RAFT_ROLE_BY_STATE.get(state, f"unknown:{state}")
        leader = self.member if state == _RAFT_LEADER_STATE else self._known_leader()
        return CoordinatorStatus(
            node_id=self.member.node_id,
            raft_identity=self.member.raft_identity,
            role=role,
            leader=leader,
            term=int(raw_status["raft_term"]),
            raft_commit_index=int(raw_status["commit_idx"]),
            raft_last_applied=int(raw_status["last_applied"]),
            control_log_sequence=_database_max_sequence(self._database_path),
            has_quorum=bool(raw_status["has_quorum"]),
            ready=self._raft.isReady(),
        )

    def commit(
        self,
        command: ControlCommand,
        *,
        timeout_seconds: float = DEFAULT_COMMAND_TIMEOUT_SECONDS,
    ) -> CommittedCommand:
        """Commit one idempotent control command through the Raft quorum."""
        if not isinstance(command, ControlCommand):
            raise ValueError("command must be a ControlCommand")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or timeout_seconds <= 0
            or not math.isfinite(timeout_seconds)
        ):
            raise ValueError("timeout_seconds must be a finite positive number")

        status = self.status()
        if status.role != "leader":
            raise NotLeaderError(status.leader)
        existing = _read_command_by_id(self._database_path, command.command_id)
        if existing is not None:
            if (
                existing.command.kind != command.kind
                or existing.payload_json != command.payload_json
            ):
                raise ValueError(
                    f"command_id {command.command_id!r} already names a "
                    "different committed command"
                )
            return existing.command

        try:
            value = self._control_log.append(
                command.command_id,
                command.kind,
                command.payload_json,
                sync=True,
                timeout=float(timeout_seconds),
            )
        except SyncObjException as error:
            reason = error.errorCode
            if reason in {
                FAIL_REASON.MISSING_LEADER,
                FAIL_REASON.NOT_LEADER,
                FAIL_REASON.LEADER_CHANGED,
            }:
                raise NotLeaderError(self._known_leader()) from error
            if reason == FAIL_REASON.QUEUE_FULL:
                raise CoordinatorUnavailableError(
                    "bounded Raft command queue is full"
                ) from error
            raise CoordinatorUnavailableError(
                f"Raft commit did not complete ({reason!r}); outcome may be unknown"
            ) from error
        if not isinstance(value, dict):
            raise CoordinatorUnavailableError(
                f"Raft returned an invalid commit response {value!r}"
            )
        return _committed_command_from_mapping(value)

    def read_log(
        self,
        *,
        after_sequence: int = 0,
        limit: int = 100,
    ) -> tuple[CommittedCommand, ...]:
        """Read a bounded page of the locally applied durable control log."""
        _require_non_negative_int(after_sequence, "after_sequence")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 1000
        ):
            raise ValueError("limit must be an integer from 1 through 1000")
        return _read_commands(self._database_path, after_sequence, limit)

    def close(self) -> None:
        """Stop Raft networking and join its single tick thread."""
        if self._closed:
            return
        self._closed = True
        self._raft.destroy_synchronous()

    def _known_leader(self) -> MemberConfig | None:
        leader_node = self._raft.getStatus().get("leader")
        if leader_node is None:
            return None
        return self.config.member_for_raft_address(str(leader_node))


def _require_port(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65535:
        raise ValueError(f"{name} must be an integer from 1 through 65535")


def _require_non_negative_int(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")


def _require_command_token(value: object, name: str, *, max_length: int) -> None:
    if (
        not isinstance(value, str)
        or len(value) > max_length
        or not _COMMAND_TOKEN_PATTERN.fullmatch(value)
    ):
        raise ValueError(
            f"{name} must contain 1-{max_length} letters, digits, dots, "
            "underscores, colons, slashes, or hyphens"
        )


def _canonical_json(value: object) -> str:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"payload must contain only finite JSON values: {error}"
        ) from error


def _load_payload_json(payload_json: str) -> dict[str, object]:
    value = json.loads(payload_json)
    if not isinstance(value, dict):
        raise ValueError("stored command payload must be a JSON object")
    return value


def _member_from_json(value: object) -> MemberConfig:
    if not isinstance(value, dict):
        raise ValueError("each cluster member must be a JSON object")
    member = MemberConfig(
        node_id=value.get("node_id"),  # type: ignore[arg-type]
        host=value.get("host"),  # type: ignore[arg-type]
        raft_port=value.get("raft_port"),  # type: ignore[arg-type]
        api_port=value.get("api_port"),  # type: ignore[arg-type]
    )
    configured_identity = value.get("raft_identity")
    if configured_identity is not None and configured_identity != member.raft_identity:
        raise ValueError(
            f"member {member.node_id!r} raft_identity "
            f"{configured_identity!r} does not match address-bound identity "
            f"{member.raft_identity!r}"
        )
    return member


def _verify_state_identity(
    state_directory: Path,
    config: ClusterConfig,
    member: MemberConfig,
) -> None:
    identity_path = state_directory / "identity.json"
    expected = {
        "node_id": member.node_id,
        "raft_identity": member.raft_identity,
        "cluster": config.to_dict(),
    }
    if identity_path.exists():
        try:
            observed = json.loads(identity_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ValueError(
                f"failed to load coordinator identity {identity_path}: {error}"
            ) from error
        if observed != expected:
            raise ValueError(
                f"state directory {state_directory} belongs to a different "
                "coordinator identity or cluster"
            )
        return
    _write_json_atomic(identity_path, expected)


def _write_json_atomic(path: Path, value: object) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    data = f"{json.dumps(value, indent=2, sort_keys=True)}\n"
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _connect_database(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=5.0)
    connection.execute("PRAGMA busy_timeout = 5000")
    connection.execute("PRAGMA synchronous = FULL")
    return connection


def _validate_raft_journal(path: Path) -> None:
    if not path.exists():
        return
    try:
        size = path.stat().st_size
        with path.open("rb") as stream:
            header = stream.read(_RAFT_JOURNAL_HEADER_BYTES)
    except OSError as error:
        raise CorruptCoordinatorStateError(
            path,
            f"failed to read Raft journal header: {error}",
        ) from error
    if size < _RAFT_JOURNAL_HEADER_BYTES or len(header) < _RAFT_JOURNAL_HEADER_BYTES:
        raise CorruptCoordinatorStateError(
            path,
            f"Raft journal is {size} bytes; expected at least "
            f"{_RAFT_JOURNAL_HEADER_BYTES}",
        )
    application_name = header[:24].rstrip(b"\0")
    if application_name != _RAFT_JOURNAL_MAGIC:
        raise CorruptCoordinatorStateError(
            path,
            f"Raft journal magic is {application_name!r}; expected "
            f"{_RAFT_JOURNAL_MAGIC!r}",
        )
    last_record_offset = int.from_bytes(header[36:40], "little")
    if not _RAFT_JOURNAL_HEADER_BYTES <= last_record_offset <= size:
        raise CorruptCoordinatorStateError(
            path,
            f"Raft journal last-record offset {last_record_offset} is outside "
            f"{_RAFT_JOURNAL_HEADER_BYTES}..{size}",
        )


def _validate_database(path: Path) -> None:
    if not path.exists():
        return
    try:
        with _connect_database(path) as connection:
            rows = connection.execute("PRAGMA quick_check").fetchall()
    except sqlite3.DatabaseError as error:
        raise CorruptCoordinatorStateError(
            path,
            f"SQLite quick_check failed: {error}",
        ) from error
    if rows != [("ok",)]:
        raise CorruptCoordinatorStateError(
            path,
            f"SQLite quick_check returned {rows!r}",
        )


def _initialize_database(path: Path) -> None:
    _validate_database(path)
    with _connect_database(path) as connection:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS committed_commands (
                sequence INTEGER PRIMARY KEY,
                command_id TEXT NOT NULL UNIQUE,
                kind TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )


def _database_max_sequence(path: Path) -> int:
    with _connect_database(path) as connection:
        row = connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) FROM committed_commands"
        ).fetchone()
    if row is None:
        return 0
    return int(row[0])


def _insert_command(
    path: Path,
    command: CommittedCommand,
    payload_json: str,
    boundary: DurableWriteBoundary,
) -> None:
    with _connect_database(path) as connection:
        boundary.before_write(path)
        connection.execute(
            """
            INSERT INTO committed_commands (
                sequence,
                command_id,
                kind,
                payload_json
            ) VALUES (?, ?, ?, ?)
            """,
            (
                command.sequence,
                command.command_id,
                command.kind,
                payload_json,
            ),
        )
        boundary.before_commit(path)


def _read_command_by_id(path: Path, command_id: str) -> _StoredCommand | None:
    with _connect_database(path) as connection:
        row = connection.execute(
            """
            SELECT sequence, command_id, kind, payload_json
            FROM committed_commands
            WHERE command_id = ?
            """,
            (command_id,),
        ).fetchone()
    if row is None:
        return None
    payload_json = str(row[3])
    return _StoredCommand(
        command=CommittedCommand(
            sequence=int(row[0]),
            command_id=str(row[1]),
            kind=str(row[2]),
            payload=_load_payload_json(payload_json),
        ),
        payload_json=payload_json,
    )


def _read_commands(
    path: Path,
    after_sequence: int,
    limit: int,
) -> tuple[CommittedCommand, ...]:
    with _connect_database(path) as connection:
        rows = connection.execute(
            """
            SELECT sequence, command_id, kind, payload_json
            FROM committed_commands
            WHERE sequence > ?
            ORDER BY sequence
            LIMIT ?
            """,
            (after_sequence, limit),
        ).fetchall()
    return tuple(
        CommittedCommand(
            sequence=int(row[0]),
            command_id=str(row[1]),
            kind=str(row[2]),
            payload=_load_payload_json(str(row[3])),
        )
        for row in rows
    )


def _committed_command_from_mapping(value: dict[str, Any]) -> CommittedCommand:
    payload = value.get("payload")
    if not isinstance(payload, dict):
        raise CoordinatorUnavailableError(
            f"Raft returned an invalid command payload {payload!r}"
        )
    sequence = value.get("sequence")
    command_id = value.get("command_id")
    kind = value.get("kind")
    if (
        isinstance(sequence, bool)
        or not isinstance(sequence, int)
        or not isinstance(command_id, str)
        or not isinstance(kind, str)
    ):
        raise CoordinatorUnavailableError(
            f"Raft returned an invalid commit response {value!r}"
        )
    return CommittedCommand(sequence, command_id, kind, payload)


@final
class _ControlPlaneLog(SyncObjConsumer):
    """Replicated state machine that applies committed commands to SQLite."""

    def __init__(
        self,
        database_path: Path,
        durable_write_boundary: DurableWriteBoundary,
    ) -> None:
        # SyncObjConsumer excludes fields present before its initializer from
        # snapshots. Runtime paths and injected dependencies must stay here.
        self._database_path = database_path
        self._durable_write_boundary = durable_write_boundary
        super().__init__()
        self._next_sequence = _database_max_sequence(database_path)

    @replicated
    def append(
        self,
        command_id: str,
        kind: str,
        payload_json: str,
    ) -> dict[str, object]:
        existing = _read_command_by_id(self._database_path, command_id)
        if existing is not None:
            self._next_sequence = max(
                self._next_sequence,
                existing.command.sequence,
            )
            return existing.command.to_dict()

        next_sequence = self._next_sequence + 1
        command = CommittedCommand(
            sequence=next_sequence,
            command_id=command_id,
            kind=kind,
            payload=_load_payload_json(payload_json),
        )
        _insert_command(
            self._database_path,
            command,
            payload_json,
            self._durable_write_boundary,
        )
        self._next_sequence = next_sequence
        return command.to_dict()


@final
@dataclass(frozen=True)
class _StoredCommand:
    command: CommittedCommand
    payload_json: str


@final
class _DirectDurableWrite:
    def before_write(self, path: Path) -> None:
        del path

    def before_commit(self, path: Path) -> None:
        del path


_DIRECT_DURABLE_WRITE = _DirectDurableWrite()


__all__ = [
    "ClusterConfig",
    "CommittedCommand",
    "ControlCommand",
    "CoordinatorStatus",
    "CoordinatorUnavailableError",
    "CorruptCoordinatorStateError",
    "DEFAULT_COMMAND_TIMEOUT_SECONDS",
    "DurableWriteBoundary",
    "MAX_COMMAND_BYTES",
    "MemberConfig",
    "NotLeaderError",
    "PersistentRaftCoordinator",
]
