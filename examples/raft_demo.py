from __future__ import annotations

from typing import TypedDict

from manyfold import Consensus, Graph
from manyfold.components import LeaderState, QuorumState, ReplicatedLog


def run_example() -> RaftDemoExampleResult:
    graph = Graph()
    consensus = Consensus.install(graph, nodes=("node-a", "node-b"))

    consensus.tick(1)
    consensus.tick(2)
    consensus.propose(1, "set mode=auto")
    consensus.propose(2, "set temp=21")

    latest_leader = consensus.latest_leader()
    latest_quorum = consensus.latest_quorum()
    latest_log = consensus.latest_log()
    assert latest_leader is not None
    assert latest_quorum is not None
    assert latest_log is not None

    return {
        "leader_state": latest_leader,
        "quorum_state": latest_quorum,
        "replicated_log": latest_log,
    }


class RaftDemoExampleResult(TypedDict):
    leader_state: LeaderState
    quorum_state: QuorumState
    replicated_log: ReplicatedLog


if __name__ == "__main__":
    print(run_example())
