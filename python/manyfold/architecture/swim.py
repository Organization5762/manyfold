"""Bounded SWIM-style failure detection over authenticated datagrams."""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import final

from ._swim_transport import (
    DEFAULT_MAX_DATAGRAM_BYTES,
    DEFAULT_MAX_INBOUND_DATAGRAMS,
    DEFAULT_MAX_PEER_CREDENTIALS,
    DEFAULT_MAX_REPLAY_MESSAGES,
    AuthenticatedDatagram,
    DatagramSocket,
    HmacDatagramTransport,
    HmacPeerCredentials,
    HmacTransportConfig,
    HmacTransportStats,
    IdentifierSource,
    RandomSource,
    ReceivedDatagram,
    SecretsIdentifierSource,
    SwimClosedError,
    SwimError,
    SwimMessageTransport,
    SwimTransportError,
    SystemRandomSource,
    UdpDatagramSocket,
    _canonical_json,
    _require_nonnegative_int,
    _require_nonnegative_number,
    _require_positive_int,
    _require_positive_number,
    _require_text,
)
from .discovery import PeerEndpoint
from .membership import (
    AuthenticatedPeerSession,
    MemberRecord,
    MembershipCapacityError,
    MembershipClosedError,
    MembershipTable,
    MembershipUpdate,
    MemberState,
    MonotonicClock,
    SystemMonotonicClock,
)
from .transport import NodeIdentity

DEFAULT_HELPER_COUNT = 3
DEFAULT_INDIRECT_TIMEOUT_SECONDS = 0.5
DEFAULT_MAX_DISSEMINATION_UPDATES = 256
DEFAULT_MAX_MESSAGE_BYTES = 512
DEFAULT_MAX_PENDING_PROBES = 32
DEFAULT_MAX_PENDING_RELAYS = 64
DEFAULT_MAX_PIGGYBACK_UPDATES = 8
DEFAULT_MAX_SEEDS = 256
DEFAULT_MAX_SEEN_REQUESTS = 4096
DEFAULT_PING_TIMEOUT_SECONDS = 0.25
DEFAULT_PROBE_INTERVAL_SECONDS = 1.0
DEFAULT_RETRANSMIT_LIMIT = 4
_PROTOCOL_VERSION = 2


@final
@dataclass(frozen=True)
class SwimConfig:
    """Deadlines and hard resource bounds for one SWIM owner."""

    probe_interval_seconds: float = DEFAULT_PROBE_INTERVAL_SECONDS
    ping_timeout_seconds: float = DEFAULT_PING_TIMEOUT_SECONDS
    indirect_timeout_seconds: float = DEFAULT_INDIRECT_TIMEOUT_SECONDS
    helper_count: int = DEFAULT_HELPER_COUNT
    max_pending_probes: int = DEFAULT_MAX_PENDING_PROBES
    max_pending_relays: int = DEFAULT_MAX_PENDING_RELAYS
    max_seeds: int = DEFAULT_MAX_SEEDS
    max_seen_requests: int = DEFAULT_MAX_SEEN_REQUESTS
    max_dissemination_updates: int = DEFAULT_MAX_DISSEMINATION_UPDATES
    max_piggyback_updates: int = DEFAULT_MAX_PIGGYBACK_UPDATES
    retransmit_limit: int = DEFAULT_RETRANSMIT_LIMIT
    max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES

    def __post_init__(self) -> None:
        for field in (
            "probe_interval_seconds",
            "ping_timeout_seconds",
            "indirect_timeout_seconds",
        ):
            object.__setattr__(
                self,
                field,
                _require_positive_number(getattr(self, field), field),
            )
        for field in (
            "helper_count",
            "max_pending_probes",
            "max_pending_relays",
            "max_seeds",
            "max_seen_requests",
            "max_dissemination_updates",
            "max_piggyback_updates",
            "retransmit_limit",
            "max_message_bytes",
        ):
            object.__setattr__(
                self,
                field,
                _require_positive_int(getattr(self, field), field),
            )


@final
@dataclass(frozen=True)
class SwimStats:
    """Protocol progress counters and current bounded-state sizes."""

    direct_probes: int
    indirect_probes: int
    successful_probes: int
    failed_probes: int
    malformed_messages: int
    duplicate_requests: int
    rejected_updates: int
    pending_probes: int
    pending_relays: int
    seeds: int
    seen_requests: int
    dissemination_updates: int


@final
@dataclass(frozen=True)
class SwimPeerSeed:
    """Expected credential-bound peer that is not a member until authenticated."""

    identity: NodeIdentity
    endpoint: PeerEndpoint
    incarnation: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "incarnation",
            _require_nonnegative_int(self.incarnation, "incarnation"),
        )


@final
class SwimMembership:
    """Externally ticked, bounded SWIM-style membership owner.

    The owner is deliberately single-threaded. Its scheduler calls ``tick``;
    the transport and membership table are owned until ``close``.
    """

    def __init__(
        self,
        membership: MembershipTable,
        transport: SwimMessageTransport,
        *,
        config: SwimConfig | None = None,
        clock: MonotonicClock | None = None,
        random_source: RandomSource | None = None,
        identifier_source: IdentifierSource | None = None,
    ) -> None:
        self._membership = membership
        self._transport = transport
        self._config = config or SwimConfig()
        self._clock = clock or SystemMonotonicClock()
        self._random = random_source or SystemRandomSource()
        self._identifiers = identifier_source or SecretsIdentifierSource()
        self._pending: dict[str, _PendingProbe] = {}
        self._relays: dict[str, _PendingRelay] = {}
        self._seeds: dict[str, SwimPeerSeed] = {}
        self._seen_order: deque[tuple[str, str, str]] = deque()
        self._seen_set: set[tuple[str, str, str]] = set()
        self._dissemination: dict[str, _DisseminationEntry] = {}
        self._dissemination_sequence = 0
        self._next_probe_at = self._now()
        self._direct_probes = 0
        self._indirect_probes = 0
        self._successful_probes = 0
        self._failed_probes = 0
        self._malformed_messages = 0
        self._duplicate_requests = 0
        self._rejected_updates = 0
        self._closed = False
        self._left = False

    @property
    def membership(self) -> MembershipTable:
        """Return the owned table for bounded snapshots and change readers."""
        return self._membership

    @property
    def is_closed(self) -> bool:
        """Return whether all owned state has been disposed."""
        return self._closed

    @property
    def stats(self) -> SwimStats:
        """Return protocol counters and retained-state sizes."""
        return SwimStats(
            direct_probes=self._direct_probes,
            indirect_probes=self._indirect_probes,
            successful_probes=self._successful_probes,
            failed_probes=self._failed_probes,
            malformed_messages=self._malformed_messages,
            duplicate_requests=self._duplicate_requests,
            rejected_updates=self._rejected_updates,
            pending_probes=len(self._pending),
            pending_relays=len(self._relays),
            seeds=len(self._seeds),
            seen_requests=len(self._seen_set),
            dissemination_updates=len(self._dissemination),
        )

    def add_seed(self, seed: SwimPeerSeed) -> bool:
        """Add a bounded probe target without admitting it as a member.

        The expected identity must be bound to a credential at the transport
        boundary. The peer enters membership only after an authenticated
        datagram arrives from that identity.
        """
        self._require_active()
        if not isinstance(seed, SwimPeerSeed):
            raise ValueError("seed must be a SwimPeerSeed")
        if seed.identity.cluster_id != self._membership.local_identity.cluster_id:
            raise ValueError(
                f"seed belongs to cluster {seed.identity.cluster_id!r}; expected "
                f"{self._membership.local_identity.cluster_id!r}"
            )
        if seed.identity == self._membership.local_identity:
            raise ValueError("seed must not use the local identity")
        if self._membership.member(seed.identity.node_id) is not None:
            return False
        if seed.identity.node_id in self._seeds:
            self._seeds[seed.identity.node_id] = seed
            return False
        if len(self._seeds) >= self._config.max_seeds:
            raise SwimError(
                f"cannot add seed {seed.identity.node_id!r}: seed limit "
                f"{self._config.max_seeds} reached"
            )
        self._seeds[seed.identity.node_id] = seed
        return True

    def add_peer(self, session: AuthenticatedPeerSession) -> MemberRecord:
        """Bootstrap one peer only after its transport credential is validated."""
        self._require_active()
        change = self._membership.observe_authenticated_session(session)
        if change is not None:
            self._queue_record(change.record)
        self._seeds.pop(session.identity.node_id, None)
        record = self._membership.member(session.identity.node_id)
        if record is None:
            raise SwimError(
                f"authenticated peer {session.identity.node_id!r} was not admitted"
            )
        return record

    def tick(self) -> SwimStats:
        """Process bounded input, deadlines, dissemination, and one new probe."""
        self._require_open()
        if self._left:
            return self.stats
        now = self._now()
        for datagram in self._transport.receive():
            self._receive_datagram(datagram, now)
        self._expire_relays(now)
        self._advance_probes(now)
        for change in self._membership.expire():
            self._queue_record(change.record)
        if now >= self._next_probe_at:
            self._start_probe(now)
            self._next_probe_at = now + self._config.probe_interval_seconds
        return self.stats

    def leave(self) -> bool:
        """Disseminate an explicit local leave once and stop protocol work."""
        self._require_open()
        if self._left:
            return False
        peers = tuple(
            record
            for record in self._membership.snapshot()
            if record.identity != self._membership.local_identity
            and record.state is MemberState.ALIVE
        )
        if not self._membership.leave_local():
            return False
        local = self._membership.local_record
        update = _update_from_record(local)
        send_error: Exception | None = None
        for peer in peers:
            try:
                self._send_message(
                    peer.endpoint,
                    "leave",
                    {"update": _encode_update(update)},
                    include_dissemination=False,
                )
            except Exception as error:
                if send_error is None:
                    send_error = error
        self._left = True
        self._pending.clear()
        self._relays.clear()
        self._seeds.clear()
        self._dissemination.clear()
        if send_error is not None:
            raise SwimTransportError(
                f"local leave completed, but dissemination failed: {send_error}"
            ) from send_error
        return True

    def close(self) -> bool:
        """Cancel protocol work and release transport and membership state."""
        if self._closed:
            return False
        self._closed = True
        self._pending.clear()
        self._relays.clear()
        self._seeds.clear()
        self._seen_order.clear()
        self._seen_set.clear()
        self._dissemination.clear()
        transport_error: Exception | None = None
        try:
            self._transport.close()
        except Exception as error:
            transport_error = error
        self._membership.close()
        if transport_error is not None:
            raise SwimTransportError(
                f"failed to close SWIM transport: {transport_error}"
            ) from transport_error
        return True

    def __enter__(self) -> "SwimMembership":
        self._require_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _receive_datagram(
        self,
        datagram: AuthenticatedDatagram,
        now: float,
    ) -> None:
        if datagram.session.identity == self._membership.local_identity:
            return
        message = self._decode_message(datagram.payload)
        if message is None:
            self._malformed_messages += 1
            return
        seed = self._seeds.get(datagram.session.identity.node_id)
        if seed is not None and datagram.session.incarnation < seed.incarnation:
            self._rejected_updates += 1
            return
        try:
            change = self._membership.observe_authenticated_session(datagram.session)
        except MembershipCapacityError:
            self._rejected_updates += 1
            return
        self._seeds.pop(datagram.session.identity.node_id, None)
        if change is not None:
            self._queue_record(change.record)
        for update in message["updates"]:
            self._apply_update(update)

        message_type = message["type"]
        if message_type == "ping":
            self._receive_ping(datagram.session, message, now)
        elif message_type == "ack":
            self._receive_ack(datagram.session, message)
        elif message_type == "ping_req":
            self._receive_ping_request(datagram.session, message, now)
        elif message_type == "leave":
            self._apply_update(message["update"])

    def _receive_ping(
        self,
        sender: AuthenticatedPeerSession,
        message: dict[str, object],
        now: float,
    ) -> None:
        del now
        probe_id = message["probe_id"]
        if not isinstance(probe_id, str):
            return
        self._remember_request(sender.identity.node_id, "ping", probe_id)
        local = self._membership.local_record
        self._send_message(
            sender.endpoint,
            "ack",
            {
                "probe_id": probe_id,
                "target_incarnation": local.incarnation,
                "target_node_id": local.identity.node_id,
            },
        )

    def _receive_ack(
        self,
        sender: AuthenticatedPeerSession,
        message: dict[str, object],
    ) -> None:
        probe_id = message["probe_id"]
        target_node_id = message["target_node_id"]
        target_incarnation = message["target_incarnation"]
        if (
            not isinstance(probe_id, str)
            or not isinstance(target_node_id, str)
            or not isinstance(target_incarnation, int)
        ):
            return

        relay = self._relays.get(probe_id)
        if relay is not None:
            if (
                sender.identity.node_id != relay.target_node_id
                or target_node_id != relay.target_node_id
                or target_incarnation < relay.target_incarnation
            ):
                return
            del self._relays[probe_id]
            self._send_message(
                relay.requester_endpoint,
                "ack",
                {
                    "probe_id": relay.requester_probe_id,
                    "target_incarnation": target_incarnation,
                    "target_node_id": target_node_id,
                },
            )
            return

        pending = self._pending.get(probe_id)
        if pending is None:
            return
        allowed_senders = {pending.target_node_id, *pending.helpers}
        if (
            sender.identity.node_id not in allowed_senders
            or target_node_id != pending.target_node_id
            or target_incarnation < pending.target_incarnation
        ):
            return
        del self._pending[probe_id]
        self._successful_probes += 1

    def _receive_ping_request(
        self,
        requester: AuthenticatedPeerSession,
        message: dict[str, object],
        now: float,
    ) -> None:
        probe_id = message["probe_id"]
        target = message["target"]
        if not isinstance(probe_id, str) or not isinstance(target, MembershipUpdate):
            return
        request_key = (requester.identity.node_id, "ping_req", probe_id)
        if request_key in self._seen_set:
            self._duplicate_requests += 1
            return
        self._retain_request(request_key)
        if len(self._relays) >= self._config.max_pending_relays:
            return
        record = self._membership.member(target.identity.node_id)
        if record is None or record.state is not MemberState.ALIVE:
            return
        helper_probe_id = self._new_probe_id()
        self._relays[helper_probe_id] = _PendingRelay(
            requester_node_id=requester.identity.node_id,
            requester_endpoint=requester.endpoint,
            requester_probe_id=probe_id,
            target_node_id=record.identity.node_id,
            target_incarnation=record.incarnation,
            deadline=now + self._config.indirect_timeout_seconds,
        )
        self._send_message(
            record.endpoint,
            "ping",
            {"probe_id": helper_probe_id},
        )

    def _apply_update(self, update: MembershipUpdate) -> None:
        try:
            change = self._membership.apply_update(update)
        except (MembershipCapacityError, MembershipClosedError):
            self._rejected_updates += 1
            return
        if change is not None:
            self._queue_record(change.record)

    def _start_probe(self, now: float) -> None:
        if len(self._pending) >= self._config.max_pending_probes:
            return
        pending_targets = {
            probe.target_node_id for probe in self._pending.values()
        }
        targets = {
            record.identity.node_id: (record.endpoint, record.incarnation)
            for record in self._membership.snapshot()
            if record.identity != self._membership.local_identity
            and record.state in (MemberState.ALIVE, MemberState.SUSPECT)
            and record.identity.node_id not in pending_targets
        }
        for seed in self._seeds.values():
            if seed.identity.node_id not in pending_targets:
                targets.setdefault(
                    seed.identity.node_id,
                    (seed.endpoint, seed.incarnation),
                )
        selected = self._random.sample(tuple(targets), 1)
        if not selected:
            return
        target_node_id = selected[0]
        target_endpoint, target_incarnation = targets[target_node_id]
        probe_id = self._new_probe_id()
        self._pending[probe_id] = _PendingProbe(
            target_node_id=target_node_id,
            target_incarnation=target_incarnation,
            target_endpoint=target_endpoint,
            phase=_ProbePhase.DIRECT,
            deadline=now + self._config.ping_timeout_seconds,
        )
        self._direct_probes += 1
        self._send_message(target_endpoint, "ping", {"probe_id": probe_id})

    def _advance_probes(self, now: float) -> None:
        for probe_id, pending in tuple(self._pending.items()):
            if now < pending.deadline:
                continue
            if pending.phase is _ProbePhase.DIRECT:
                helpers = self._select_helpers(pending.target_node_id)
                if helpers:
                    pending.phase = _ProbePhase.INDIRECT
                    pending.deadline = now + self._config.indirect_timeout_seconds
                    pending.helpers = tuple(record.identity.node_id for record in helpers)
                    target = MembershipUpdate(
                        identity=NodeIdentity(
                            self._membership.local_identity.cluster_id,
                            pending.target_node_id,
                        ),
                        endpoint=pending.target_endpoint,
                        incarnation=pending.target_incarnation,
                        state=MemberState.ALIVE,
                    )
                    for helper in helpers:
                        self._send_message(
                            helper.endpoint,
                            "ping_req",
                            {
                                "probe_id": probe_id,
                                "target": _encode_update(target),
                            },
                        )
                    self._indirect_probes += 1
                    continue
            del self._pending[probe_id]
            self._failed_probes += 1
            if self._membership.mark_suspect(
                pending.target_node_id,
                incarnation=pending.target_incarnation,
            ):
                record = self._membership.member(pending.target_node_id)
                if record is not None:
                    self._queue_record(record)

    def _select_helpers(self, target_node_id: str) -> tuple[MemberRecord, ...]:
        records = {
            record.identity.node_id: record
            for record in self._membership.snapshot()
            if record.identity != self._membership.local_identity
            and record.identity.node_id != target_node_id
            and record.state is MemberState.ALIVE
        }
        selected = self._random.sample(
            tuple(records),
            self._config.helper_count,
        )
        return tuple(records[node_id] for node_id in selected)

    def _expire_relays(self, now: float) -> None:
        for probe_id, relay in tuple(self._relays.items()):
            if now >= relay.deadline:
                del self._relays[probe_id]

    def _queue_record(self, record: MemberRecord) -> None:
        update = _update_from_record(record)
        existing = self._dissemination.get(record.identity.node_id)
        if existing is not None and not _update_is_newer(update, existing.update):
            return
        if (
            existing is None
            and len(self._dissemination)
            >= self._config.max_dissemination_updates
        ):
            evicted_node_id = max(
                self._dissemination,
                key=lambda node_id: (
                    self._dissemination[node_id].transmissions,
                    -self._dissemination[node_id].sequence,
                ),
            )
            del self._dissemination[evicted_node_id]
        self._dissemination_sequence += 1
        self._dissemination[record.identity.node_id] = _DisseminationEntry(
            update=update,
            transmissions=0,
            sequence=self._dissemination_sequence,
        )

    def _send_message(
        self,
        endpoint: PeerEndpoint,
        message_type: str,
        fields: dict[str, object],
        *,
        include_dissemination: bool = True,
    ) -> None:
        message = {
            "type": message_type,
            "version": _PROTOCOL_VERSION,
            **fields,
        }
        selected: list[str] = []
        updates: list[dict[str, object]] = []
        if include_dissemination:
            entries = sorted(
                self._dissemination.items(),
                key=lambda item: (item[1].transmissions, item[1].sequence),
            )
            for node_id, entry in entries:
                if len(updates) == self._config.max_piggyback_updates:
                    break
                candidate = [*updates, _encode_update(entry.update)]
                message["updates"] = candidate
                if len(_canonical_json(message)) > self._config.max_message_bytes:
                    continue
                updates = candidate
                selected.append(node_id)
        message["updates"] = updates
        payload = _canonical_json(message)
        if len(payload) > self._config.max_message_bytes:
            raise SwimTransportError(
                f"SWIM message is {len(payload)} bytes; limit is "
                f"{self._config.max_message_bytes}"
            )
        local_incarnation = self._membership.local_record.incarnation
        self._transport.send(
            endpoint,
            payload,
            local_incarnation=local_incarnation,
        )
        for node_id in selected:
            entry = self._dissemination.get(node_id)
            if entry is None:
                continue
            entry.transmissions += 1
            if entry.transmissions >= self._config.retransmit_limit:
                del self._dissemination[node_id]

    def _decode_message(self, payload: bytes) -> dict[str, object] | None:
        if len(payload) > self._config.max_message_bytes:
            return None
        try:
            raw = json.loads(payload)
            if not isinstance(raw, dict):
                return None
            message_type = raw.get("type")
            if message_type not in {"ack", "leave", "ping", "ping_req"}:
                return None
            if raw.get("version") != _PROTOCOL_VERSION:
                return None
            allowed_fields = {
                "ping": {"probe_id", "type", "updates", "version"},
                "ack": {
                    "probe_id",
                    "target_incarnation",
                    "target_node_id",
                    "type",
                    "updates",
                    "version",
                },
                "ping_req": {
                    "probe_id",
                    "target",
                    "type",
                    "updates",
                    "version",
                },
                "leave": {"type", "update", "updates", "version"},
            }
            if set(raw) != allowed_fields[message_type]:
                return None
            raw_updates = raw.get("updates")
            if not isinstance(raw_updates, list):
                return None
            if len(raw_updates) > self._config.max_piggyback_updates:
                return None
            updates = tuple(_decode_update(value) for value in raw_updates)
            decoded: dict[str, object] = {
                "type": message_type,
                "updates": updates,
            }
            if message_type in {"ack", "ping", "ping_req"}:
                decoded["probe_id"] = _require_text(
                    raw.get("probe_id"),
                    "probe_id",
                    max_length=64,
                )
            if message_type == "ack":
                decoded["target_node_id"] = _require_text(
                    raw.get("target_node_id"),
                    "target_node_id",
                    max_length=256,
                )
                decoded["target_incarnation"] = _require_nonnegative_int(
                    raw.get("target_incarnation"),
                    "target_incarnation",
                )
            elif message_type == "ping_req":
                decoded["target"] = _decode_update(raw.get("target"))
            elif message_type == "leave":
                decoded["update"] = _decode_update(raw.get("update"))
            return decoded
        except (
            json.JSONDecodeError,
            TypeError,
            UnicodeDecodeError,
            ValueError,
        ):
            return None

    def _remember_request(
        self,
        sender_node_id: str,
        message_type: str,
        probe_id: str,
    ) -> None:
        request = (sender_node_id, message_type, probe_id)
        if request in self._seen_set:
            self._duplicate_requests += 1
            return
        self._retain_request(request)

    def _retain_request(self, request: tuple[str, str, str]) -> None:
        while len(self._seen_order) >= self._config.max_seen_requests:
            expired = self._seen_order.popleft()
            self._seen_set.remove(expired)
        self._seen_order.append(request)
        self._seen_set.add(request)

    def _new_probe_id(self) -> str:
        for _ in range(4):
            probe_id = _require_text(
                self._identifiers.next_id(),
                "probe_id",
                max_length=64,
            )
            if probe_id not in self._pending and probe_id not in self._relays:
                return probe_id
        raise SwimError("identifier source repeated an active probe identifier")

    def _now(self) -> float:
        return _require_nonnegative_number(self._clock.now(), "clock value")

    def _require_open(self) -> None:
        if self._closed:
            raise SwimClosedError("SWIM membership is closed")

    def _require_active(self) -> None:
        self._require_open()
        if self._left:
            raise SwimClosedError("local member has left the SWIM cluster")


def _encode_update(update: MembershipUpdate) -> dict[str, object]:
    return {
        "cluster": update.identity.cluster_id,
        "endpoint": {
            "host": update.endpoint.host,
            "port": update.endpoint.port,
        },
        "incarnation": update.incarnation,
        "instance": update.identity.instance_id,
        "node": update.identity.node_id,
        "state": update.state.value,
    }


def _decode_update(value: object) -> MembershipUpdate:
    if not isinstance(value, dict):
        raise ValueError("membership update must be an object")
    if set(value) != {
        "cluster",
        "endpoint",
        "incarnation",
        "instance",
        "node",
        "state",
    }:
        raise ValueError("membership update fields do not match the protocol")
    endpoint = value["endpoint"]
    if not isinstance(endpoint, dict) or set(endpoint) != {"host", "port"}:
        raise ValueError("membership endpoint fields do not match the protocol")
    return MembershipUpdate(
        identity=NodeIdentity(
            _require_text(value["cluster"], "cluster", max_length=256),
            _require_text(value["node"], "node", max_length=256),
            _require_text(value["instance"], "instance", max_length=256),
        ),
        endpoint=PeerEndpoint(
            _require_text(endpoint["host"], "host", max_length=512),
            endpoint["port"],
        ),
        incarnation=_require_nonnegative_int(
            value["incarnation"],
            "incarnation",
        ),
        state=MemberState(_require_text(value["state"], "state", max_length=16)),
    )


def _update_from_record(record: MemberRecord) -> MembershipUpdate:
    return MembershipUpdate(
        identity=record.identity,
        endpoint=record.endpoint,
        incarnation=record.incarnation,
        state=record.state,
    )


def _update_is_newer(
    candidate: MembershipUpdate,
    existing: MembershipUpdate,
) -> bool:
    if candidate.incarnation != existing.incarnation:
        return candidate.incarnation > existing.incarnation
    precedence = {
        MemberState.ALIVE: 0,
        MemberState.SUSPECT: 1,
        MemberState.DEAD: 2,
        MemberState.LEFT: 3,
    }
    return precedence[candidate.state] > precedence[existing.state]


class _ProbePhase(str, Enum):
    DIRECT = "direct"
    INDIRECT = "indirect"


@dataclass
class _PendingProbe:
    target_node_id: str
    target_incarnation: int
    target_endpoint: PeerEndpoint
    phase: _ProbePhase
    deadline: float
    helpers: tuple[str, ...] = ()


@dataclass(frozen=True)
class _PendingRelay:
    requester_node_id: str
    requester_endpoint: PeerEndpoint
    requester_probe_id: str
    target_node_id: str
    target_incarnation: int
    deadline: float


@dataclass
class _DisseminationEntry:
    update: MembershipUpdate
    transmissions: int
    sequence: int


__all__ = [
    "AuthenticatedDatagram",
    "DEFAULT_HELPER_COUNT",
    "DEFAULT_INDIRECT_TIMEOUT_SECONDS",
    "DEFAULT_MAX_DATAGRAM_BYTES",
    "DEFAULT_MAX_DISSEMINATION_UPDATES",
    "DEFAULT_MAX_INBOUND_DATAGRAMS",
    "DEFAULT_MAX_MESSAGE_BYTES",
    "DEFAULT_MAX_PEER_CREDENTIALS",
    "DEFAULT_MAX_PENDING_PROBES",
    "DEFAULT_MAX_PENDING_RELAYS",
    "DEFAULT_MAX_PIGGYBACK_UPDATES",
    "DEFAULT_MAX_REPLAY_MESSAGES",
    "DEFAULT_MAX_SEEDS",
    "DEFAULT_MAX_SEEN_REQUESTS",
    "DEFAULT_PING_TIMEOUT_SECONDS",
    "DEFAULT_PROBE_INTERVAL_SECONDS",
    "DEFAULT_RETRANSMIT_LIMIT",
    "DatagramSocket",
    "HmacDatagramTransport",
    "HmacPeerCredentials",
    "HmacTransportConfig",
    "HmacTransportStats",
    "IdentifierSource",
    "RandomSource",
    "ReceivedDatagram",
    "SecretsIdentifierSource",
    "SwimClosedError",
    "SwimConfig",
    "SwimError",
    "SwimMembership",
    "SwimMessageTransport",
    "SwimPeerSeed",
    "SwimStats",
    "SwimTransportError",
    "SystemRandomSource",
    "UdpDatagramSocket",
]
