"""Bounded production scenarios for distributed release qualification."""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path

from manyfold.architecture.discovery import DnsDiscovery, DnsSeed, PeerEndpoint
from manyfold.architecture.membership import (
    AuthenticatedPeerSession,
    MembershipConfig,
    MembershipTable,
)
from manyfold.architecture.transport import NodeIdentity
from manyfold.cluster import ClusterConfig, DevelopmentCluster, MemberConfig
from manyfold.cluster.network import (
    DISCONNECT_FAULT_LAYER,
    NetworkProtocolConfig,
)

from .distributed_qualification_heart import run_heart_scenarios
from .distributed_qualification_types import ScenarioResult, blocked, failed, result

MAX_RESOURCE_RSS_GROWTH_KIB = 64 * 1024
MAX_RESOURCE_THREADS = 8
MAX_RESOURCE_FILE_DESCRIPTORS = 64
MAX_RAFT_APPLY_BACKLOG = 128
MAX_CONCURRENT_CLUSTER_PROCESSES = 3
_POLL_SECONDS = 0.05
_CORE_ORDER = (
    "first_node_boot",
    "three_node_convergence",
    "simultaneous_cold_start",
    "duplicate_identities",
    "stale_and_malformed_mdns_dns_candidates",
    "partition_and_asymmetric_loss",
    "leader_kill",
    "process_restart",
    "unavailable_quorum",
    "corrupt_and_truncated_local_state",
    "disk_full_and_write_failure",
    "deterministic_shutdown",
)


def run_release_scenarios(
    output_dir: Path,
    *,
    timeout_seconds: float,
    heart_artifact_dir: Path | None,
) -> tuple[ScenarioResult, ...]:
    """Run the short production matrix."""
    results = {
        scenario.name: scenario
        for scenario in _cluster_scenarios(
            output_dir / "cluster",
            timeout_seconds=timeout_seconds,
        )
    }
    discovery = _discovery_scenario()
    results[discovery.name] = discovery
    results["disk_full_and_write_failure"] = blocked(
        "disk_full_and_write_failure",
        "PersistentRaftCoordinator has no explicit injectable durable-write "
        "boundary for deterministic ENOSPC or partial-write qualification.",
        durable_files=[
            "committed.sqlite3",
            "raft.journal",
            "raft.snapshot",
        ],
        safe_injection_hook=None,
    )
    ordered = tuple(results[name] for name in _CORE_ORDER)
    return ordered + run_heart_scenarios(
        heart_artifact_dir,
        output_dir=output_dir,
    )


def run_soak_scenario(
    output_dir: Path,
    *,
    duration_seconds: int,
    sample_interval_seconds: float,
    timeout_seconds: float,
) -> ScenarioResult:
    """Measure resource bounds and convergence for 10-30 minutes."""
    cluster = DevelopmentCluster.create(output_dir / "soak")
    samples: list[dict[str, object]] = []
    started = time.monotonic()
    path = output_dir / "soak-samples.jsonl"
    try:
        cluster.start()
        while time.monotonic() - started < duration_seconds:
            index = len(samples) + 1
            committed_at = time.monotonic()
            command = cluster.commit(
                "qualification.soak",
                {"sample": index},
                command_id=f"soak-{index}",
                timeout_seconds=timeout_seconds,
            )
            leader = cluster.wait_for_leader(timeout_seconds=timeout_seconds)
            statuses = {
                member.node_id: cluster.status(member.node_id)
                for member in cluster.members
            }
            resources = {
                member.node_id: _process_resources(
                    _required_process_id(cluster, member.node_id)
                )
                for member in cluster.members
            }
            sample = {
                "sample": index,
                "elapsed_seconds": time.monotonic() - started,
                "leader": leader,
                "sequence": command["sequence"],
                "commit_latency_ms": (time.monotonic() - committed_at) * 1000,
                "raft_apply_backlog": max(
                    int(status["commit_index"]) - int(status["last_applied"])
                    for status in statuses.values()
                ),
                "resources": resources,
            }
            samples.append(sample)
            with path.open("a", encoding="utf-8") as stream:
                json.dump(sample, stream, sort_keys=True)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            remaining = sample_interval_seconds - (
                time.monotonic() - committed_at
            )
            if remaining > 0:
                time.sleep(remaining)
    except Exception as error:
        return failed("optional_soak", "Soak did not complete", error)
    finally:
        cluster.stop()
    metrics = _soak_metrics(samples)
    passed = (
        metrics["complete"] is True
        and int(metrics["rss_growth_kib"]) <= MAX_RESOURCE_RSS_GROWTH_KIB
        and int(metrics["max_threads"]) <= MAX_RESOURCE_THREADS
        and int(metrics["max_fds"]) <= MAX_RESOURCE_FILE_DESCRIPTORS
        and int(metrics["max_apply_backlog"]) <= MAX_RAFT_APPLY_BACKLOG
    )
    return result(
        "optional_soak",
        passed,
        "The bounded cluster stays within RSS, thread, descriptor, apply-backlog, "
        "and convergence limits for the configured soak duration.",
        evidence={"samples": len(samples), **metrics},
    )


def _cluster_scenarios(
    root: Path,
    *,
    timeout_seconds: float,
) -> tuple[ScenarioResult, ...]:
    names = tuple(name for name in _CORE_ORDER if name not in {
        "stale_and_malformed_mdns_dns_candidates",
        "disk_full_and_write_failure",
    })
    cluster = DevelopmentCluster.create(
        root,
        network=NetworkProtocolConfig(layers=(DISCONNECT_FAULT_LAYER,)),
    )
    results: dict[str, ScenarioResult] = {}
    node_ids = tuple(member.node_id for member in cluster.members)
    try:
        first_pid = cluster.start_node(node_ids[0])
        first_ready = _wait_for(
            lambda: cluster.status(node_ids[0]).get("role") in {
                "follower",
                "candidate",
                "leader",
            },
            timeout_seconds,
        )
        results["first_node_boot"] = result(
            "first_node_boot",
            first_ready,
            "One production coordinator boots with identity-bound durable state "
            "without claiming a false three-node quorum.",
            evidence={
                "process_id": first_pid,
                "has_quorum": cluster.status(node_ids[0]).get("has_quorum"),
            },
        )

        spawn_started = time.monotonic()
        for node_id in node_ids[1:]:
            cluster.start_node(node_id)
        spawn_ms = (time.monotonic() - spawn_started) * 1000
        convergence_started = time.monotonic()
        leader = cluster.wait_for_leader(timeout_seconds=timeout_seconds)
        convergence_ms = (time.monotonic() - convergence_started) * 1000
        statuses = {node_id: cluster.status(node_id) for node_id in node_ids}
        results["three_node_convergence"] = result(
            "three_node_convergence",
            sum(status["role"] == "leader" for status in statuses.values()) == 1
            and all(status["has_quorum"] is True for status in statuses.values()),
            "Three real coordinator processes converge on one quorum-backed leader.",
            evidence={
                "leader": leader,
                "convergence_latency_ms": convergence_ms,
                "statuses": statuses,
            },
        )
        results["simultaneous_cold_start"] = result(
            "simultaneous_cold_start",
            spawn_ms < 1000,
            "Cold coordinator process spawns overlap and converge within the bounded "
            "election deadline.",
            evidence={"spawn_elapsed_ms": spawn_ms, "leader": leader},
        )
        same_pid = cluster.start_node(node_ids[0])
        duplicate_rejected = _duplicate_config_rejected(cluster.config)
        results["duplicate_identities"] = result(
            "duplicate_identities",
            same_pid == first_pid and duplicate_rejected,
            "Duplicate process start reuses its owner and duplicate configured "
            "identities are rejected.",
            evidence={
                "first_process_id": first_pid,
                "repeated_process_id": same_pid,
                "duplicate_config_rejected": duplicate_rejected,
            },
        )

        cluster.commit(
            "qualification.baseline",
            {"phase": "before-loss"},
            command_id="before-loss",
            timeout_seconds=timeout_seconds,
        )
        follower = next(node for node in node_ids if node != leader)
        cluster.disconnect_node(follower)
        disconnected = _wait_for(
            lambda: cluster.status(follower).get("has_quorum") is False,
            timeout_seconds,
        )
        cluster.reconnect_node(follower)
        cluster.wait_for_log_length(follower, 1, timeout_seconds=timeout_seconds)
        results["partition_and_asymmetric_loss"] = result(
            "partition_and_asymmetric_loss",
            disconnected,
            "A one-node asymmetric Raft disconnect loses quorum locally and catches "
            "up without process restart after healing.",
            evidence={"partitioned_node": follower, "caught_up": True},
        )

        leader_pid = cluster.process_id(leader)
        cluster.kill_node(leader)
        replacement = cluster.wait_for_leader(
            timeout_seconds=timeout_seconds,
            excluded_node_ids=frozenset({leader}),
        )
        recovered = cluster.commit(
            "qualification.recovery",
            {"phase": "after-leader-kill"},
            command_id="after-leader-kill",
            timeout_seconds=timeout_seconds,
        )
        results["leader_kill"] = result(
            "leader_kill",
            replacement != leader and recovered["sequence"] == 2,
            "A surviving quorum elects a different leader and commits after abrupt "
            "leader loss.",
            evidence={"killed": leader, "replacement": replacement},
        )
        restarted_pid = cluster.start_node(leader)
        log = cluster.wait_for_log_length(
            leader,
            2,
            timeout_seconds=timeout_seconds,
        )
        results["process_restart"] = result(
            "process_restart",
            restarted_pid != leader_pid
            and [item["command_id"] for item in log]
            == ["before-loss", "after-leader-kill"],
            "A killed coordinator restarts with the same identity and catches up its "
            "durable committed log.",
            evidence={"old_pid": leader_pid, "new_pid": restarted_pid},
        )

        current_leader = cluster.wait_for_leader(timeout_seconds=timeout_seconds)
        stopped = [node for node in node_ids if node != current_leader]
        for node in stopped:
            cluster.stop_node(node)
        lost_quorum = _wait_for(
            lambda: cluster.status(current_leader).get("has_quorum") is False,
            timeout_seconds,
        )
        rejected = False
        try:
            cluster.commit(
                "qualification.no-quorum",
                {"invalid": True},
                command_id="no-quorum",
                timeout_seconds=1,
            )
        except TimeoutError:
            rejected = True
        results["unavailable_quorum"] = result(
            "unavailable_quorum",
            lost_quorum and rejected,
            "A one-node minority loses quorum and cannot commit.",
            evidence={"remaining_node": current_leader, "commit_rejected": rejected},
        )
        for node in stopped:
            cluster.start_node(node)
        cluster.wait_for_leader(timeout_seconds=timeout_seconds)
        cluster.stop()
        results["corrupt_and_truncated_local_state"] = _state_damage(
            cluster,
            timeout_seconds=timeout_seconds,
        )
    except Exception as error:
        for name in names:
            results.setdefault(
                name,
                failed(name, "Shared production cluster scenario failed", error),
            )
    finally:
        cluster.stop()
    results["deterministic_shutdown"] = result(
        "deterministic_shutdown",
        all(cluster.process_id(node) is None for node in node_ids),
        "DevelopmentCluster deterministically stops and reaps every child.",
        evidence={
            "process_ids_after_stop": {
                node: cluster.process_id(node) for node in node_ids
            }
        },
    )
    return tuple(results[name] for name in names)


def _state_damage(
    cluster: DevelopmentCluster,
    *,
    timeout_seconds: float,
) -> ScenarioResult:
    node_ids = tuple(member.node_id for member in cluster.members)
    journal = cluster.state_directory(node_ids[0]) / "raft.journal"
    identity = cluster.state_directory(node_ids[1]) / "identity.json"
    try:
        journal_bytes = journal.read_bytes()
        journal.write_bytes(journal_bytes[: max(40, len(journal_bytes) // 2)])
        cluster.start()
        cluster.wait_for_leader(timeout_seconds=timeout_seconds)
        recovered = all(
            len(
                cluster.wait_for_log_length(
                    node,
                    2,
                    timeout_seconds=timeout_seconds,
                )
            )
            >= 2
            for node in node_ids
        )
        cluster.stop()
        identity_bytes = identity.read_bytes()
        identity.write_bytes(b"{")
        cluster.start_node(node_ids[1])
        rejected = cluster.wait_for_node_exit(
            node_ids[1],
            timeout_seconds=timeout_seconds,
        ) != 0
        identity.write_bytes(identity_bytes)
        return result(
            "corrupt_and_truncated_local_state",
            recovered and rejected,
            "A truncated journal rejoins from the surviving quorum while malformed "
            "identity state fails closed.",
            evidence={
                "truncated_journal_recovered": recovered,
                "malformed_identity_rejected": rejected,
            },
        )
    except Exception as error:
        return failed(
            "corrupt_and_truncated_local_state",
            "State-damage recovery failed",
            error,
        )
    finally:
        cluster.stop()


def _discovery_scenario() -> ScenarioResult:
    try:
        report = DnsDiscovery(
            (DnsSeed("stale.example", 7443), DnsSeed("mixed.example", 7443)),
            resolver=_AddressResolver(),
            max_candidates=4,
        ).discover()
        membership = MembershipTable(
            NodeIdentity("qualification", "node-a", "instance-a"),
            PeerEndpoint("127.0.0.1", 7443),
            local_incarnation=2,
            config=MembershipConfig(max_members=4, max_changes=8),
        )
        membership.heartbeat(
            AuthenticatedPeerSession(
                NodeIdentity("qualification", "node-b", "instance-b"),
                PeerEndpoint("127.0.0.2", 7443),
                2,
            )
        )
        stale = membership.heartbeat(
            AuthenticatedPeerSession(
                NodeIdentity("qualification", "node-b", "stale"),
                PeerEndpoint("127.0.0.9", 7443),
                1,
            )
        )
        membership.close()
        passed = (
            [candidate.endpoint.host for candidate in report.candidates]
            == ["192.0.2.10"]
            and len(report.failures) == 2
            and stale.incarnation == 2
            and stale.endpoint.host == "127.0.0.2"
        )
        return result(
            "stale_and_malformed_mdns_dns_candidates",
            passed,
            "Discovery preserves valid bounded candidates, reports stale/malformed "
            "sources, and membership ignores stale incarnations.",
            evidence={
                "candidates": [
                    candidate.endpoint.host for candidate in report.candidates
                ],
                "failures": [failure.message for failure in report.failures],
                "stale_incarnation_observed": stale.incarnation,
            },
        )
    except Exception as error:
        return failed(
            "stale_and_malformed_mdns_dns_candidates",
            "Discovery qualification failed",
            error,
        )


def _duplicate_config_rejected(config: ClusterConfig) -> bool:
    first, second, third = config.members
    try:
        ClusterConfig(
            (
                first,
                second,
                MemberConfig(
                    first.node_id,
                    third.host,
                    third.raft_port,
                    third.api_port,
                ),
            ),
            config.network,
        )
    except ValueError:
        return True
    return False


def _wait_for(predicate: object, timeout_seconds: float) -> bool:
    if not callable(predicate):
        raise TypeError("predicate must be callable")
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            if predicate():
                return True
        except (ConnectionError, OSError, RuntimeError):
            pass
        time.sleep(_POLL_SECONDS)
    return False


def _required_process_id(cluster: DevelopmentCluster, node_id: str) -> int:
    process_id = cluster.process_id(node_id)
    if process_id is None:
        raise RuntimeError(f"coordinator is not running: {node_id}")
    return process_id


def _process_resources(process_id: int) -> dict[str, int | None]:
    completed = subprocess.run(
        ("ps", "-o", "rss=,thcount=", "-p", str(process_id)),
        check=True,
        capture_output=True,
        text=True,
    )
    values = completed.stdout.split()
    lsof = subprocess.run(
        ("lsof", "-a", "-p", str(process_id), "-F", "f"),
        check=False,
        capture_output=True,
        text=True,
    )
    return {
        "rss_kib": int(values[0]) if values else None,
        "thread_count": int(values[1]) if len(values) > 1 else None,
        "file_descriptor_count": (
            sum(line.startswith("f") for line in lsof.stdout.splitlines())
            if lsof.returncode == 0
            else None
        ),
    }


def _soak_metrics(samples: list[dict[str, object]]) -> dict[str, object]:
    resources = [
        item
        for sample in samples
        for item in dict(sample["resources"]).values()
        if isinstance(item, dict)
    ]
    rss = [int(item["rss_kib"]) for item in resources if item["rss_kib"] is not None]
    threads = [
        int(item["thread_count"])
        for item in resources
        if item["thread_count"] is not None
    ]
    fds = [
        int(item["file_descriptor_count"])
        for item in resources
        if item["file_descriptor_count"] is not None
    ]
    expected = len(samples) * MAX_CONCURRENT_CLUSTER_PROCESSES
    return {
        "complete": len(rss) == len(threads) == len(fds) == expected,
        "rss_growth_kib": max(rss) - min(rss) if rss else -1,
        "max_threads": max(threads) if threads else -1,
        "max_fds": max(fds) if fds else -1,
        "max_apply_backlog": max(
            int(sample["raft_apply_backlog"]) for sample in samples
        ),
        "max_commit_latency_ms": max(
            float(sample["commit_latency_ms"]) for sample in samples
        ),
    }


class _AddressResolver:
    def resolve(self, hostname: str) -> tuple[str, ...]:
        if hostname == "stale.example":
            raise OSError("stale DNS seed")
        return ("not-an-ip", "192.0.2.10")
