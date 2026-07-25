"""Verify real installed-wheel Heart consumer artifacts."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from urllib.parse import unquote, urlparse

from .distributed_qualification_types import ScenarioResult, blocked, failed, result

HEART_ARTIFACT_FILENAMES = {
    "signer": "signer.json",
    "mesh": "mesh.json",
    "coordination": "coordination.json",
}
_HOT_TOPICS = {
    "heart.frame_tick",
    "heart.rendered_frame",
    "heart.microphone.level",
    "heart.input",
}
_HOT_DATA = {
    "debug",
    "frame_tick",
    "microphone_sample",
    "navigation_event",
    "rendered_frame",
    "sensor_sample",
}
_HASH_CHUNK_BYTES = 1024 * 1024


def run_heart_scenarios(
    artifact_dir: Path | None,
    *,
    output_dir: Path,
) -> tuple[ScenarioResult, ...]:
    """Verify and preserve final Heart signer, mesh, and coordination evidence."""
    names = _heart_scenario_names()
    if artifact_dir is None:
        reason = (
            "No Heart installed-wheel artifacts were supplied; a ManyFold-only "
            "result cannot satisfy this release gate."
        )
        return tuple(blocked(name, reason) for name in names)
    try:
        artifacts = {
            name: _read_json(artifact_dir / filename)
            for name, filename in HEART_ARTIFACT_FILENAMES.items()
        }
        preserved = _preserve(artifact_dir, output_dir)
        mesh = _verify_mesh(artifacts["mesh"])
        signer = _verify_signer(artifacts["signer"])
        coordination = _verify_coordination(artifacts["coordination"])
    except (OSError, TypeError, ValueError) as error:
        return tuple(
            failed(name, "Heart installed-wheel evidence is invalid", error)
            for name in names
        )

    common_mesh = {**preserved["mesh"], **mesh}
    common_signer = {**preserved["signer"], **signer}
    common_coordination = {**preserved["coordination"], **coordination}
    return (
        result(
            "authenticated_transport",
            True,
            "Heart's two real processes authenticate the candidate TcpTransport "
            "through signer-enrolled mutual TLS before membership.",
            evidence=common_mesh,
        ),
        result(
            "certificate_expiry_and_rotation",
            True,
            "Short-lived credentials renew after signer restart and become unusable "
            "after expiry when renewal remains unavailable.",
            evidence=common_signer,
        ),
        result(
            "node_bootstrap_lifecycle",
            True,
            "Discovery remains untrusted until authentication, replacement identity "
            "converges, subscriptions recover, and shutdown is clean.",
            evidence=common_mesh,
        ),
        result(
            "machine_signer_identity_and_shared_processes",
            True,
            "Two local client processes share one machine signer and receive distinct "
            "short-lived credential serials.",
            evidence=common_signer,
        ),
        result(
            "machine_signer_authorization_and_availability",
            True,
            "Unauthorized local clients and unavailable signer startup fail closed.",
            evidence=common_signer,
        ),
        result(
            "machine_signer_credential_lifecycle",
            True,
            "Credential renewal, outage use, signer restart, rotation, and expiry "
            "follow the shipped lifecycle.",
            evidence=common_signer,
        ),
        result(
            "machine_signer_bounds_and_key_isolation",
            True,
            "Signer IPC permissions are owner-only and client processes never open "
            "durable private-key files.",
            evidence=common_signer,
        ),
        result(
            "heart_navigation_sensor_and_hot_paths",
            True,
            "Heart navigation and a low-rate sensor recover across process loss while "
            "frame, rendered-frame, debug, and microphone paths remain local.",
            evidence=common_mesh,
        ),
        result(
            "heart_machine_signer_lifecycle",
            True,
            "Heart passes real signer bootstrap, authorization, renewal, restart, "
            "expiry, and durable-key isolation.",
            evidence=common_signer,
        ),
        result(
            "heart_raft_rpc_leader_failure",
            True,
            "Heart world/device state survives leader loss and restart through real "
            "three-process Raft, RPC, and exactly-once durable application.",
            evidence=common_coordination,
        ),
    )


def _heart_scenario_names() -> tuple[str, ...]:
    return (
        "authenticated_transport",
        "certificate_expiry_and_rotation",
        "node_bootstrap_lifecycle",
        "machine_signer_identity_and_shared_processes",
        "machine_signer_authorization_and_availability",
        "machine_signer_credential_lifecycle",
        "machine_signer_bounds_and_key_isolation",
        "heart_navigation_sensor_and_hot_paths",
        "heart_machine_signer_lifecycle",
        "heart_raft_rpc_leader_failure",
    )


def _verify_mesh(value: dict[str, object]) -> dict[str, object]:
    navigation = _object(value, "navigation_counts")
    sensor = _object(value, "sensor_counts")
    story = _object(value, "story")
    policy = _list(value, "topic_policy")
    local = _strings(value, "local_only_topics")
    by_topic = {
        item["topic"]: item
        for item in policy
        if isinstance(item, dict) and isinstance(item.get("topic"), str)
    }
    valid = (
        value.get("authentication")
        == "machine_signer_enrollment_mutual_tls_identity_uri"
        and value.get("process_count") == 2
        and navigation
        and sensor
        and all(int(count) >= 1 for count in (*navigation.values(), *sensor.values()))
        and story
        and all(flag is True for flag in story.values())
        and local == _HOT_TOPICS
        and not _list(value, "durable_delivery_topics")
        and not _list(value, "raft_topics")
        and all(
            by_topic.get(topic, {}).get("delivery") == "local"
            and by_topic[topic].get("durable") is False
            and by_topic[topic].get("raft") is False
            for topic in _HOT_TOPICS
        )
    )
    if not valid:
        raise ValueError("Heart mesh artifact does not prove every required outcome")
    return {
        "authentication": value["authentication"],
        "process_count": value["process_count"],
        "navigation_counts": navigation,
        "sensor_counts": sensor,
        "story": story,
        "local_only_topics": sorted(local),
    }


def _verify_signer(value: dict[str, object]) -> dict[str, object]:
    bootstrap = _object(value, "bootstrap")
    lifecycle = _object(value, "restart_renewal")
    initial = _object(lifecycle, "initial")
    outage = _object(lifecycle, "during_outage")
    restarted = _object(lifecycle, "after_restart")
    expired = _object(lifecycle, "after_expiry")
    closed = _object(lifecycle, "closed")
    unauthorized = _object(value, "unauthorized")
    unavailable = _object(value, "unavailable_bootstrap")
    permissions = _object(value, "ipc_permissions")
    serials = _list(bootstrap, "serial_numbers")
    command_args = [
        argument
        for command in _list(value, "commands")
        if isinstance(command, list)
        for argument in command
        if isinstance(argument, str)
    ]
    rejected = (unauthorized, unavailable)
    valid = (
        value.get("protocol") == "heart.manyfold-signer-qualification"
        and bootstrap.get("client_processes") == 2
        and _list(bootstrap, "states") == ["ready", "ready"]
        and len(serials) == len(set(serials)) == 2
        and bootstrap.get("durable_private_key_opened_by_clients") is False
        and not _list(bootstrap, "durable_private_key_open_matches")
        and initial.get("state") == "ready"
        and outage.get("state") == "renewal_failed"
        and outage.get("is_usable") is True
        and restarted.get("state") == "ready"
        and int(_required(restarted, "generation"))
        > int(_required(initial, "generation"))
        and restarted.get("serial_number") != initial.get("serial_number")
        and expired.get("state") == "expired"
        and expired.get("is_usable") is False
        and closed.get("state") == "closed"
        and closed.get("durable_private_key_opened") is False
        and all(
            item.get("state") == "rejected"
            and item.get("credential_state") == "unavailable"
            and item.get("durable_private_key_opened") is False
            for item in rejected
        )
        and permissions
        == {
            "socket_mode": "0600",
            "socket_parent_mode": "0700",
            "state_directory_mode": "0700",
        }
        and value.get("enrollment_token_file_removed") is True
        and "--token-file" in command_args
        and "--token" not in command_args
    )
    if not valid:
        raise ValueError("Heart signer artifact does not prove every required outcome")
    return {
        "client_processes": 2,
        "distinct_client_serials": 2,
        "durable_private_key_opened_by_clients": False,
        "lifecycle_states": {
            key: _object(lifecycle, key).get("state")
            for key in (
                "initial",
                "during_outage",
                "after_restart",
                "after_expiry",
                "closed",
            )
        },
        "ipc_permissions": permissions,
    }


def _verify_coordination(value: dict[str, object]) -> dict[str, object]:
    installation = _object(value, "manyfold_installation")
    direct_url = _object(installation, "direct_url")
    wheel_path = _local_file_url(str(_required(direct_url, "url")))
    raft = _object(value, "raft")
    delivery = _object(value, "durable_delivery")
    boundary = _object(value, "boundary")
    revisions = _object(raft, "node_revisions")
    valid = (
        value.get("schema_version") == 1
        and installation.get("install_kind") == "wheel"
        and installation.get("distribution_version") == "0.1.42"
        and raft.get("node_count") == 3
        and raft.get("leader_changed") is True
        and raft.get("restarted_process_changed") is True
        and raft.get("initial_leader") != raft.get("recovered_leader")
        and len(revisions) == 3
        and set(revisions.values()) == {2}
        and delivery.get("receiver_restarted") is True
        and delivery.get("applied_count") == 1
        and delivery.get("receiver_acknowledgements") == 1
        and delivery.get("duplicate_exposed_after_ack") is False
        and delivery.get("sender_outbox_items") == 0
        and _strings(boundary, "excluded_hot_path_data") == _HOT_DATA
    )
    if not valid:
        raise ValueError("Heart coordination artifact is incomplete")
    return {
        "candidate_wheel_sha256": _sha256(wheel_path),
        "manyfold_installation": installation,
        "initial_leader": raft["initial_leader"],
        "recovered_leader": raft["recovered_leader"],
        "node_revisions": revisions,
        "durable_delivery": delivery,
        "excluded_hot_path_data": sorted(_HOT_DATA),
    }


def _read_json(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Heart artifact must be an object: {path}")
    return value


def _preserve(source: Path, output: Path) -> dict[str, dict[str, str]]:
    destination = output / "heart"
    destination.mkdir(parents=True)
    evidence: dict[str, dict[str, str]] = {}
    for name, filename in HEART_ARTIFACT_FILENAMES.items():
        target = destination / filename
        shutil.copyfile(source / filename, target)
        evidence[name] = {
            "artifact": str(Path("heart") / filename),
            "sha256": _sha256(target),
        }
    return evidence


def _local_file_url(url: str) -> Path:
    parsed = urlparse(url)
    if parsed.scheme != "file" or parsed.netloc not in ("", "localhost"):
        raise ValueError("candidate wheel provenance must be a local file URL")
    path = Path(unquote(parsed.path))
    if path.suffix != ".whl" or not path.is_file():
        raise ValueError(f"candidate wheel does not exist: {path}")
    return path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(_HASH_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def _required(value: dict[str, object], key: str) -> object:
    if key not in value:
        raise ValueError(f"missing Heart artifact field: {key}")
    return value[key]


def _object(value: dict[str, object], key: str) -> dict[str, object]:
    item = _required(value, key)
    if not isinstance(item, dict):
        raise ValueError(f"Heart artifact field must be an object: {key}")
    return item


def _list(value: dict[str, object], key: str) -> list[object]:
    item = _required(value, key)
    if not isinstance(item, list):
        raise ValueError(f"Heart artifact field must be a list: {key}")
    return item


def _strings(value: dict[str, object], key: str) -> set[str]:
    items = _list(value, key)
    if not all(isinstance(item, str) for item in items):
        raise ValueError(f"Heart artifact field must contain strings: {key}")
    return set(items)
