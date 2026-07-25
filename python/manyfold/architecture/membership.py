"""Bounded authenticated peer membership for distributed Manyfold runtimes."""

from __future__ import annotations

import math
import threading
import time
from collections import deque
from dataclasses import dataclass, replace
from enum import Enum
from typing import Protocol, final

from .discovery import PeerEndpoint

DEFAULT_DEAD_RETENTION_SECONDS = 300.0
DEFAULT_LEASE_SECONDS = 15.0
DEFAULT_MAX_CHANGES = 256
DEFAULT_MAX_MEMBERS = 256
DEFAULT_SUSPECT_SECONDS = 5.0


class MonotonicClock(Protocol):
    """Monotonic clock used for lease and state-transition deadlines."""

    def now(self) -> float: ...


@final
class SystemMonotonicClock:
    """Production clock backed by ``time.monotonic``."""

    def now(self) -> float:
        """Return monotonic seconds."""
        return time.monotonic()


class MemberState(str, Enum):
    """Lifecycle state of one authenticated cluster member."""

    ALIVE = "alive"
    SUSPECT = "suspect"
    DEAD = "dead"
    LEFT = "left"


@final
@dataclass(frozen=True)
class PeerIdentity:
    """Cluster and node identity proven by an authenticated transport."""

    cluster_id: str
    node_id: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "cluster_id",
            _require_text(self.cluster_id, "cluster_id"),
        )
        object.__setattr__(self, "node_id", _require_text(self.node_id, "node_id"))


@final
@dataclass(frozen=True)
class AuthenticatedPeerSession:
    """Identity and endpoint output from a successful peer authentication.

    Construction is a trust boundary: discovery alone must never construct this
    value. The session transport is responsible for validating cluster and node
    credentials before producing it.
    """

    identity: PeerIdentity
    endpoint: PeerEndpoint
    incarnation: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "incarnation",
            _require_nonnegative_int(self.incarnation, "incarnation"),
        )


@final
@dataclass(frozen=True)
class MembershipConfig:
    """Resource and time bounds for a membership table."""

    lease_seconds: float = DEFAULT_LEASE_SECONDS
    suspect_seconds: float = DEFAULT_SUSPECT_SECONDS
    dead_retention_seconds: float = DEFAULT_DEAD_RETENTION_SECONDS
    max_members: int = DEFAULT_MAX_MEMBERS
    max_changes: int = DEFAULT_MAX_CHANGES

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "lease_seconds",
            _require_positive_number(self.lease_seconds, "lease_seconds"),
        )
        object.__setattr__(
            self,
            "suspect_seconds",
            _require_positive_number(self.suspect_seconds, "suspect_seconds"),
        )
        object.__setattr__(
            self,
            "dead_retention_seconds",
            _require_nonnegative_number(
                self.dead_retention_seconds,
                "dead_retention_seconds",
            ),
        )
        object.__setattr__(
            self,
            "max_members",
            _require_positive_int(self.max_members, "max_members"),
        )
        object.__setattr__(
            self,
            "max_changes",
            _require_positive_int(self.max_changes, "max_changes"),
        )


@final
@dataclass(frozen=True)
class MemberRecord:
    """Current state and bounded lifecycle deadline of one member."""

    identity: PeerIdentity
    endpoint: PeerEndpoint
    incarnation: int
    state: MemberState
    state_changed_at: float
    deadline: float | None


@final
@dataclass(frozen=True)
class MembershipChange:
    """One bounded state change for a future dissemination backend."""

    sequence: int
    record: MemberRecord
    reason: str


class MembershipError(RuntimeError):
    """Base error for membership operations."""


@final
class MembershipCapacityError(MembershipError):
    """Raised when a new member would exceed the configured hard bound."""


@final
class MembershipClosedError(MembershipError):
    """Raised when an operation targets a closed or departed table."""


@final
class MembershipHistoryGap(MembershipError):
    """Raised when a change reader has fallen behind bounded retention."""


@final
class PeerIdentityError(MembershipError):
    """Raised when an authenticated session does not belong to this cluster."""


@final
class MembershipTable:
    """Thread-safe bounded membership state for authenticated peers.

    The table does not perform discovery, authentication, networking, or
    background scheduling. A runtime calls ``heartbeat`` after authenticating a
    peer and drives ``expire`` from its own scheduler.
    """

    def __init__(
        self,
        local_identity: PeerIdentity,
        local_endpoint: PeerEndpoint,
        *,
        local_incarnation: int = 0,
        config: MembershipConfig | None = None,
        clock: MonotonicClock | None = None,
    ) -> None:
        self._config = config or MembershipConfig()
        self._clock = clock or SystemMonotonicClock()
        self._lock = threading.RLock()
        self._local_identity = local_identity
        self._members: dict[str, MemberRecord] = {}
        self._changes: deque[MembershipChange] = deque(
            maxlen=self._config.max_changes
        )
        self._next_sequence = 1
        self._closed = False
        self._has_left = False

        now = self._now()
        local_record = MemberRecord(
            identity=local_identity,
            endpoint=local_endpoint,
            incarnation=_require_nonnegative_int(
                local_incarnation,
                "local_incarnation",
            ),
            state=MemberState.ALIVE,
            state_changed_at=now,
            deadline=None,
        )
        self._members[local_identity.node_id] = local_record
        self._append_change(local_record, "local-started")

    @property
    def local_identity(self) -> PeerIdentity:
        """Return the local authenticated identity."""
        return self._local_identity

    @property
    def is_closed(self) -> bool:
        """Return whether resources have been disposed."""
        with self._lock:
            return self._closed

    @property
    def is_participating(self) -> bool:
        """Return whether the local node may accept membership updates."""
        with self._lock:
            return not self._closed and not self._has_left

    @property
    def latest_change_sequence(self) -> int:
        """Return the latest assigned change sequence."""
        with self._lock:
            self._require_open()
            return self._next_sequence - 1

    def heartbeat(self, session: AuthenticatedPeerSession) -> MemberRecord:
        """Admit or renew an authenticated peer lease.

        A newer incarnation supersedes every earlier state. At the same
        incarnation, a heartbeat can refute suspicion or death but cannot undo
        an explicit leave.
        """
        with self._lock:
            self._require_participating()
            self._validate_session(session)
            now = self._now()
            self._expire_locked(now)
            existing = self._members.get(session.identity.node_id)
            if existing is not None:
                if session.incarnation < existing.incarnation:
                    return existing
                if (
                    session.incarnation == existing.incarnation
                    and existing.state is MemberState.LEFT
                ):
                    return existing
            elif len(self._members) >= self._config.max_members:
                raise MembershipCapacityError(
                    "cannot admit peer "
                    f"{session.identity.node_id!r}: membership limit "
                    f"{self._config.max_members} reached"
                )

            record = MemberRecord(
                identity=session.identity,
                endpoint=session.endpoint,
                incarnation=session.incarnation,
                state=MemberState.ALIVE,
                state_changed_at=now
                if existing is None
                or existing.state is not MemberState.ALIVE
                or existing.incarnation != session.incarnation
                or existing.endpoint != session.endpoint
                else existing.state_changed_at,
                deadline=now + self._config.lease_seconds,
            )
            self._members[session.identity.node_id] = record
            if existing is None:
                self._append_change(record, "peer-admitted")
            elif (
                existing.state is not MemberState.ALIVE
                or existing.incarnation != record.incarnation
                or existing.endpoint != record.endpoint
            ):
                self._append_change(record, "peer-heartbeat-refuted-state")
            return record

    def mark_suspect(self, node_id: str, *, incarnation: int) -> bool:
        """Mark a matching live incarnation suspect after a failed probe."""
        with self._lock:
            self._require_participating()
            node_id = _require_text(node_id, "node_id")
            incarnation = _require_nonnegative_int(incarnation, "incarnation")
            if node_id == self._local_identity.node_id:
                return False
            now = self._now()
            self._expire_locked(now)
            existing = self._members.get(node_id)
            if (
                existing is None
                or existing.incarnation != incarnation
                or existing.state is not MemberState.ALIVE
            ):
                return False
            record = replace(
                existing,
                state=MemberState.SUSPECT,
                state_changed_at=now,
                deadline=now + self._config.suspect_seconds,
            )
            self._members[node_id] = record
            self._append_change(record, "probe-failed")
            return True

    def mark_dead(self, node_id: str, *, incarnation: int) -> bool:
        """Mark a matching live or suspect incarnation dead."""
        with self._lock:
            self._require_participating()
            node_id = _require_text(node_id, "node_id")
            incarnation = _require_nonnegative_int(incarnation, "incarnation")
            if node_id == self._local_identity.node_id:
                return False
            now = self._now()
            self._expire_locked(now)
            existing = self._members.get(node_id)
            if (
                existing is None
                or existing.incarnation != incarnation
                or existing.state not in (MemberState.ALIVE, MemberState.SUSPECT)
            ):
                return False
            self._transition_terminal(
                existing,
                state=MemberState.DEAD,
                now=now,
                reason="failure-confirmed",
            )
            return True

    def leave_peer(self, session: AuthenticatedPeerSession) -> bool:
        """Record an authenticated peer's explicit leave."""
        with self._lock:
            self._require_participating()
            self._validate_session(session)
            now = self._now()
            self._expire_locked(now)
            existing = self._members.get(session.identity.node_id)
            if existing is not None and session.incarnation < existing.incarnation:
                return False
            if (
                existing is not None
                and session.incarnation == existing.incarnation
                and existing.state is MemberState.LEFT
            ):
                return False
            if existing is None and len(self._members) >= self._config.max_members:
                raise MembershipCapacityError(
                    "cannot record leave for peer "
                    f"{session.identity.node_id!r}: membership limit "
                    f"{self._config.max_members} reached"
                )
            record = MemberRecord(
                identity=session.identity,
                endpoint=session.endpoint,
                incarnation=session.incarnation,
                state=MemberState.LEFT,
                state_changed_at=now,
                deadline=now + self._config.dead_retention_seconds,
            )
            self._members[session.identity.node_id] = record
            self._append_change(record, "peer-left")
            return True

    def leave_local(self) -> bool:
        """Explicitly leave the cluster and reject subsequent updates."""
        with self._lock:
            self._require_open()
            if self._has_left:
                return False
            now = self._now()
            local = self._members[self._local_identity.node_id]
            record = replace(
                local,
                state=MemberState.LEFT,
                state_changed_at=now,
                deadline=now + self._config.dead_retention_seconds,
            )
            self._members[self._local_identity.node_id] = record
            self._has_left = True
            self._append_change(record, "local-left")
            return True

    def expire(self) -> tuple[MembershipChange, ...]:
        """Apply due lease, suspicion, and terminal-retention deadlines."""
        with self._lock:
            self._require_open()
            return self._expire_locked(self._now())

    def member(self, node_id: str) -> MemberRecord | None:
        """Return one current member record without advancing time."""
        with self._lock:
            self._require_open()
            return self._members.get(_require_text(node_id, "node_id"))

    def snapshot(self) -> tuple[MemberRecord, ...]:
        """Return a deterministic point-in-time membership snapshot."""
        with self._lock:
            self._require_open()
            return tuple(
                sorted(
                    self._members.values(),
                    key=lambda record: record.identity.node_id,
                )
            )

    def changes_since(self, sequence: int) -> tuple[MembershipChange, ...]:
        """Return retained changes after ``sequence`` or report a retention gap."""
        with self._lock:
            self._require_open()
            sequence = _require_nonnegative_int(sequence, "sequence")
            if not self._changes:
                return ()
            oldest = self._changes[0].sequence
            if sequence < oldest - 1:
                raise MembershipHistoryGap(
                    f"change sequence {sequence} precedes oldest retained "
                    f"sequence {oldest}; take a fresh snapshot"
                )
            return tuple(
                change for change in self._changes if change.sequence > sequence
            )

    def close(self) -> bool:
        """Dispose the table and release all retained records and changes."""
        with self._lock:
            if self._closed:
                return False
            self._closed = True
            self._members.clear()
            self._changes.clear()
            return True

    def __enter__(self) -> "MembershipTable":
        self._require_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _append_change(self, record: MemberRecord, reason: str) -> MembershipChange:
        change = MembershipChange(
            sequence=self._next_sequence,
            record=record,
            reason=reason,
        )
        self._next_sequence += 1
        self._changes.append(change)
        return change

    def _expire_locked(self, now: float) -> tuple[MembershipChange, ...]:
        changes: list[MembershipChange] = []
        for node_id, existing in tuple(self._members.items()):
            if node_id == self._local_identity.node_id or existing.deadline is None:
                continue
            if now < existing.deadline:
                continue
            if existing.state is MemberState.ALIVE:
                record = replace(
                    existing,
                    state=MemberState.SUSPECT,
                    state_changed_at=now,
                    deadline=now + self._config.suspect_seconds,
                )
                self._members[node_id] = record
                changes.append(self._append_change(record, "lease-expired"))
            elif existing.state is MemberState.SUSPECT:
                changes.append(
                    self._transition_terminal(
                        existing,
                        state=MemberState.DEAD,
                        now=now,
                        reason="suspicion-expired",
                    )
                )
            elif existing.state in (MemberState.DEAD, MemberState.LEFT):
                del self._members[node_id]
                removal = replace(existing, deadline=None)
                changes.append(self._append_change(removal, "record-expired"))
        return tuple(changes)

    def _transition_terminal(
        self,
        existing: MemberRecord,
        *,
        state: MemberState,
        now: float,
        reason: str,
    ) -> MembershipChange:
        record = replace(
            existing,
            state=state,
            state_changed_at=now,
            deadline=now + self._config.dead_retention_seconds,
        )
        self._members[existing.identity.node_id] = record
        return self._append_change(record, reason)

    def _validate_session(self, session: AuthenticatedPeerSession) -> None:
        if not isinstance(session, AuthenticatedPeerSession):
            raise ValueError("session must be an AuthenticatedPeerSession")
        if session.identity.cluster_id != self._local_identity.cluster_id:
            raise PeerIdentityError(
                "authenticated peer belongs to cluster "
                f"{session.identity.cluster_id!r}; expected "
                f"{self._local_identity.cluster_id!r}"
            )
        if session.identity.node_id == self._local_identity.node_id:
            raise PeerIdentityError(
                "authenticated peer claimed the local node_id "
                f"{self._local_identity.node_id!r}"
            )

    def _now(self) -> float:
        return _require_nonnegative_number(self._clock.now(), "clock value")

    def _require_open(self) -> None:
        if self._closed:
            raise MembershipClosedError("membership table is closed")

    def _require_participating(self) -> None:
        self._require_open()
        if self._has_left:
            raise MembershipClosedError("local member has left the cluster")


def _require_nonnegative_int(value: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _require_nonnegative_number(value: float, field: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValueError(f"{field} must be a finite non-negative number")
    return float(value)


def _require_positive_int(value: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _require_positive_number(value: float, field: str) -> float:
    value = _require_nonnegative_number(value, field)
    if value == 0:
        raise ValueError(f"{field} must be positive")
    return value


def _require_text(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


__all__ = [
    "AuthenticatedPeerSession",
    "DEFAULT_DEAD_RETENTION_SECONDS",
    "DEFAULT_LEASE_SECONDS",
    "DEFAULT_MAX_CHANGES",
    "DEFAULT_MAX_MEMBERS",
    "DEFAULT_SUSPECT_SECONDS",
    "MemberRecord",
    "MemberState",
    "MembershipCapacityError",
    "MembershipChange",
    "MembershipClosedError",
    "MembershipConfig",
    "MembershipError",
    "MembershipHistoryGap",
    "MembershipTable",
    "MonotonicClock",
    "PeerIdentity",
    "PeerIdentityError",
    "SystemMonotonicClock",
]
