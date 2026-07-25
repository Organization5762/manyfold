from __future__ import annotations

import unittest

from manyfold.architecture import (
    AuthenticatedPeerSession,
    MembershipCapacityError,
    MembershipClosedError,
    MembershipConfig,
    MembershipHistoryGap,
    MembershipTable,
    MemberState,
    NodeIdentity,
    PeerEndpoint,
    PeerIdentityError,
)
from manyfold.architecture.membership import MembershipUpdate


class ArchitectureMembershipTests(unittest.TestCase):
    def test_lease_moves_alive_to_suspect_to_dead_then_releases_record(self) -> None:
        clock = _ManualClock()
        membership = _membership(clock)
        peer = _peer()
        membership.heartbeat(peer)

        clock.advance(10.0)
        lease_changes = membership.expire()
        clock.advance(5.0)
        suspect_changes = membership.expire()
        clock.advance(20.0)
        removal_changes = membership.expire()

        self.assertEqual(lease_changes[0].record.state, MemberState.SUSPECT)
        self.assertEqual(suspect_changes[0].record.state, MemberState.DEAD)
        self.assertEqual(removal_changes[0].reason, "record-expired")
        self.assertIsNone(membership.member("node-b"))
        self.assertEqual(
            tuple(record.identity.node_id for record in membership.snapshot()),
            ("node-a",),
        )

    def test_heartbeat_refutes_suspicion_and_ignores_stale_incarnation(self) -> None:
        clock = _ManualClock()
        membership = _membership(clock)
        first = _peer(incarnation=2)
        membership.heartbeat(first)
        self.assertTrue(membership.mark_suspect("node-b", incarnation=2))

        alive = membership.heartbeat(first)
        stale = membership.heartbeat(_peer(incarnation=1))

        self.assertEqual(alive.state, MemberState.ALIVE)
        self.assertEqual(alive.incarnation, 2)
        self.assertEqual(stale, alive)

    def test_new_incarnation_can_rejoin_after_explicit_leave(self) -> None:
        membership = _membership(_ManualClock())
        peer = _peer(incarnation=2)
        membership.heartbeat(peer)

        self.assertTrue(membership.leave_peer(peer))
        same_incarnation = membership.heartbeat(peer)
        rejoined = membership.heartbeat(_peer(incarnation=3))

        self.assertEqual(same_incarnation.state, MemberState.LEFT)
        self.assertEqual(rejoined.state, MemberState.ALIVE)
        self.assertEqual(rejoined.incarnation, 3)

    def test_authenticated_session_must_match_cluster_and_not_local_node(self) -> None:
        membership = _membership(_ManualClock())

        with self.assertRaisesRegex(PeerIdentityError, "other-cluster"):
            membership.heartbeat(
                AuthenticatedPeerSession(
                    NodeIdentity("other-cluster", "node-b"),
                    PeerEndpoint("10.0.0.2", 7443),
                    0,
                )
            )
        with self.assertRaisesRegex(PeerIdentityError, "local node_id"):
            membership.heartbeat(
                AuthenticatedPeerSession(
                    NodeIdentity("cluster-a", "node-a"),
                    PeerEndpoint("10.0.0.9", 7443),
                    0,
                )
            )

    def test_failed_probe_can_mark_matching_incarnation_dead(self) -> None:
        membership = _membership(_ManualClock())
        membership.heartbeat(_peer(incarnation=2))

        self.assertFalse(membership.mark_suspect("node-b", incarnation=1))
        self.assertTrue(membership.mark_suspect("node-b", incarnation=2))
        self.assertTrue(membership.mark_dead("node-b", incarnation=2))
        self.assertEqual(membership.member("node-b").state, MemberState.DEAD)
        self.assertFalse(membership.mark_dead("node-b", incarnation=2))

    def test_member_and_change_retention_are_hard_bounded(self) -> None:
        clock = _ManualClock()
        membership = _membership(
            clock,
            config=MembershipConfig(
                lease_seconds=10,
                suspect_seconds=5,
                dead_retention_seconds=20,
                max_members=2,
                max_changes=3,
            ),
        )
        peer = _peer()
        membership.heartbeat(peer)

        for _ in range(100):
            clock.advance(0.01)
            membership.heartbeat(peer)

        self.assertEqual(len(membership.snapshot()), 2)
        self.assertLessEqual(len(membership.changes_since(0)), 3)
        with self.assertRaisesRegex(MembershipCapacityError, "limit 2"):
            membership.heartbeat(
                AuthenticatedPeerSession(
                    NodeIdentity("cluster-a", "node-c"),
                    PeerEndpoint("10.0.0.3", 7443),
                    0,
                )
            )

        membership.mark_suspect("node-b", incarnation=0)
        membership.mark_dead("node-b", incarnation=0)
        with self.assertRaises(MembershipHistoryGap):
            membership.changes_since(0)

    def test_expired_terminal_record_releases_capacity(self) -> None:
        clock = _ManualClock()
        membership = _membership(
            clock,
            config=MembershipConfig(
                lease_seconds=10,
                suspect_seconds=5,
                dead_retention_seconds=1,
                max_members=2,
                max_changes=8,
            ),
        )
        membership.heartbeat(_peer())
        membership.leave_peer(_peer())
        clock.advance(1)
        membership.expire()

        admitted = membership.heartbeat(
            AuthenticatedPeerSession(
                NodeIdentity("cluster-a", "node-c"),
                PeerEndpoint("10.0.0.3", 7443),
                0,
            )
        )

        self.assertEqual(admitted.identity.node_id, "node-c")

    def test_local_leave_and_close_have_explicit_lifecycle(self) -> None:
        membership = _membership(_ManualClock())

        self.assertTrue(membership.leave_local())
        self.assertFalse(membership.leave_local())
        self.assertFalse(membership.is_participating)
        with self.assertRaisesRegex(MembershipClosedError, "has left"):
            membership.heartbeat(_peer())

        self.assertTrue(membership.close())
        self.assertFalse(membership.close())
        self.assertTrue(membership.is_closed)
        with self.assertRaisesRegex(MembershipClosedError, "closed"):
            membership.snapshot()

    def test_context_manager_disposes_membership(self) -> None:
        membership = _membership(_ManualClock())

        with membership as active:
            active.heartbeat(_peer())
            self.assertEqual(len(active.snapshot()), 2)

        self.assertTrue(membership.is_closed)

    def test_swim_update_precedence_rejects_stale_and_weaker_claims(self) -> None:
        membership = _membership(_ManualClock())
        membership.observe_authenticated_session(_peer(incarnation=2))

        suspect = membership.apply_update(
            _update(incarnation=2, state=MemberState.SUSPECT)
        )
        stale_alive = membership.apply_update(
            _update(incarnation=2, state=MemberState.ALIVE)
        )
        stale_dead = membership.apply_update(
            _update(incarnation=1, state=MemberState.DEAD)
        )
        left = membership.apply_update(
            _update(incarnation=2, state=MemberState.LEFT)
        )

        self.assertIsNotNone(suspect)
        self.assertIsNone(stale_alive)
        self.assertIsNone(stale_dead)
        self.assertIsNotNone(left)
        self.assertEqual(membership.member("node-b").state, MemberState.LEFT)

    def test_local_suspicion_self_refutes_above_reported_incarnation(self) -> None:
        membership = _membership(_ManualClock())

        change = membership.apply_update(
            MembershipUpdate(
                NodeIdentity("cluster-a", "node-a"),
                PeerEndpoint("10.0.0.1", 7443),
                incarnation=7,
                state=MemberState.SUSPECT,
            )
        )

        self.assertIsNotNone(change)
        self.assertEqual(change.reason, "local-refuted-state")
        self.assertEqual(membership.local_record.incarnation, 8)
        self.assertEqual(membership.local_record.state, MemberState.ALIVE)

    def test_authenticated_observation_does_not_clear_swim_suspicion(self) -> None:
        membership = _membership(_ManualClock())
        membership.observe_authenticated_session(_peer(incarnation=2))
        membership.apply_update(_update(incarnation=2, state=MemberState.SUSPECT))

        change = membership.observe_authenticated_session(_peer(incarnation=2))

        self.assertIsNone(change)
        self.assertEqual(membership.member("node-b").state, MemberState.SUSPECT)


def _membership(
    clock: _ManualClock,
    *,
    config: MembershipConfig | None = None,
) -> MembershipTable:
    return MembershipTable(
        NodeIdentity("cluster-a", "node-a"),
        PeerEndpoint("10.0.0.1", 7443),
        config=config
        or MembershipConfig(
            lease_seconds=10,
            suspect_seconds=5,
            dead_retention_seconds=20,
            max_members=8,
            max_changes=32,
        ),
        clock=clock,
    )


def _peer(*, incarnation: int = 0) -> AuthenticatedPeerSession:
    return AuthenticatedPeerSession(
        NodeIdentity("cluster-a", "node-b"),
        PeerEndpoint("10.0.0.2", 7443),
        incarnation,
    )


def _update(*, incarnation: int, state: MemberState) -> MembershipUpdate:
    return MembershipUpdate(
        NodeIdentity("cluster-a", "node-b"),
        PeerEndpoint("10.0.0.2", 7443),
        incarnation,
        state,
    )


class _ManualClock:
    def __init__(self) -> None:
        self.current = 0.0

    def now(self) -> float:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += seconds


if __name__ == "__main__":
    unittest.main()
