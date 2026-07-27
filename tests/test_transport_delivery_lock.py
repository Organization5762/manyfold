from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from queue import Queue
from threading import Event, Thread

from manyfold.architecture._transport_delivery_journal import _DeliveryJournal
from manyfold.architecture._transport_delivery_journal_errors import _JournalError
from manyfold.architecture._transport_delivery_policy import (
    DeliveryConfig,
    TopicDeliveryPolicy,
)
from manyfold.architecture._transport_delivery_records import _OutboxRecord
from manyfold.architecture.transport import (
    FrameKind,
    NodeIdentity,
    ReconnectPolicy,
    TcpAddress,
    TcpTransport,
    TransportConfig,
    TransportMessage,
    TransportSecurity,
)
from manyfold.architecture.transport_delivery import (
    DeliveryClosed,
    DeliveryError,
    DurableDelivery,
)

from tests.test_support import subprocess_test_env


class TransportDeliveryLockTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory()
        self._root = Path(self._temporary_directory.name)

    def tearDown(self) -> None:
        self._temporary_directory.cleanup()

    def test_file_identity_lock_excludes_child_and_reopens_after_write(
        self,
    ) -> None:
        original = self._root / "original.sqlite3"
        alias = self._root / "renamed.sqlite3"
        owner = _DeliveryJournal(self._config(original))
        try:
            first_id = owner.next_message_id()
            config = self._config(original)
            policy = config.topic_policies[0]
            owner.insert_outbox(
                _OutboxRecord(
                    "committed",
                    "events",
                    "append",
                    None,
                    int(FrameKind.PUBSUB),
                    None,
                    b"value",
                    0,
                    policy.max_attempts,
                ),
                created_at=1.0,
                expires_at=5.0,
                now=1.0,
                policy=policy,
            )
            self.assertEqual(owner.stats().outbox_items, 1)
            with self.assertRaisesRegex(_JournalError, "already owned"):
                _DeliveryJournal(self._config(original))
            target = original
            if os.name != "nt":
                os.link(original, alias)
                original.unlink()
                target = alias
            alternate_tmp = self._root / "child-tmp"
            alternate_tmp.mkdir()
            environment = subprocess_test_env()
            environment.update(
                {
                    "TMPDIR": str(alternate_tmp),
                    "TEMP": str(alternate_tmp),
                    "TMP": str(alternate_tmp),
                }
            )
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    _SECOND_OWNER_PROBE,
                    str(target),
                ],
                check=False,
                capture_output=True,
                text=True,
                env=environment,
                timeout=5.0,
            )
            self.assertEqual(
                result.returncode,
                0,
                msg=f"stdout={result.stdout!r} stderr={result.stderr!r}",
            )
        finally:
            owner.close()
        reopened = _DeliveryJournal(self._config(target))
        try:
            self.assertNotEqual(reopened.next_message_id(), first_id)
        finally:
            reopened.close()

    def test_owner_churn_leaves_no_lock_artifact_or_fd_tail(self) -> None:
        baseline_entries = set(self._root.iterdir())
        baseline_fds = _fd_count()
        holder_processes: list[subprocess.Popen[bytes]] = []
        for index in range(32):
            path = self._root / f"churn-{index}.sqlite3"
            owner = _DeliveryJournal(self._config(path))
            holder_processes.append(owner._owner_lock._process)
            owner.close()
            path.unlink()

        self.assertEqual(set(self._root.iterdir()), baseline_entries)
        self.assertTrue(
            all(process.poll() is not None for process in holder_processes)
        )
        if baseline_fds is not None:
            self.assertTrue(
                _wait_for(
                    lambda: _fd_count() is not None
                    and _fd_count() <= baseline_fds,
                    timeout=1.0,
                )
            )

    def test_concurrent_startup_has_exactly_one_live_owner(self) -> None:
        path = self._root / "concurrent.sqlite3"
        processes = [
            subprocess.Popen(
                [sys.executable, "-c", _CONCURRENT_OWNER, str(path)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=subprocess_test_env(),
            )
            for _ in range(2)
        ]
        try:
            armed_results: Queue[
                tuple[subprocess.Popen[str], str]
            ] = Queue()
            armed_readers = [
                Thread(
                    target=_read_process_status,
                    args=(process, armed_results),
                )
                for process in processes
            ]
            for reader in armed_readers:
                reader.start()
            armed = [armed_results.get(timeout=5.0) for _ in processes]
            self.assertTrue(all(status == "ARMED" for _, status in armed))
            for reader in armed_readers:
                reader.join(timeout=1.0)
                self.assertFalse(reader.is_alive())
            for process in processes:
                if process.stdin is None:
                    self.fail("concurrent owner stdin is unavailable")
                process.stdin.write("s")
                process.stdin.flush()
            results: Queue[tuple[subprocess.Popen[str], str]] = Queue()
            readers = [
                Thread(
                    target=_read_process_status,
                    args=(process, results),
                )
                for process in processes
            ]
            for reader in readers:
                reader.start()
            observed = [results.get(timeout=5.0) for _ in processes]
            statuses = [status for _, status in observed]
            self.assertCountEqual(statuses, ["READY", "OWNED"])
            winner = next(
                process for process, status in observed if status == "READY"
            )
            if winner.stdin is None:
                self.fail("winning owner stdin is unavailable")
            winner.stdin.write("x")
            winner.stdin.flush()
            for process in processes:
                if process.stdin is not None:
                    process.stdin.close()
                process.wait(timeout=5.0)
                stderr = (
                    ""
                    if process.stderr is None
                    else process.stderr.read()
                )
                self.assertEqual(process.returncode, 0, msg=stderr)
                if process.stdout is not None:
                    process.stdout.close()
                if process.stderr is not None:
                    process.stderr.close()
            for reader in readers:
                reader.join(timeout=1.0)
                self.assertFalse(reader.is_alive())
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=5.0)
                for stream in (
                    process.stdin,
                    process.stdout,
                    process.stderr,
                ):
                    if stream is not None and not stream.closed:
                        stream.close()
        reopened = _DeliveryJournal(self._config(path))
        reopened.close()

    def test_parent_hard_exit_releases_holder_and_journal_lock(self) -> None:
        path = self._root / "hard-exit.sqlite3"
        result = subprocess.run(
            [sys.executable, "-c", _CRASH_OWNER, str(path)],
            check=False,
            capture_output=True,
            text=True,
            env=subprocess_test_env(),
            timeout=5.0,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        holder_pid = int(result.stdout.strip())
        self.assertTrue(
            _wait_for(
                lambda: _external_holder_exited(holder_pid, path),
                timeout=2.0,
            )
        )
        reopened = _DeliveryJournal(self._config(path))
        reopened.close()

    def test_holder_reports_non_contention_lock_failure(self) -> None:
        path = self._root / "invalid-offset.sqlite3"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "manyfold.architecture._transport_delivery_lock_holder",
                str(path),
                "-1",
                "test-token",
                "manyfold-delivery-owner-lock",
            ],
            check=False,
            capture_output=True,
            text=True,
            env=subprocess_test_env(),
            timeout=5.0,
        )

        self.assertEqual(result.returncode, 3, msg=result.stderr)
        fields = result.stdout.strip().split()
        self.assertEqual(fields[:2], ["test-token", "error"])
        self.assertRegex(result.stdout, r"errno=\d+")

    def test_holder_loss_before_commit_rolls_back_transaction(self) -> None:
        path = self._root / "commit-owner-loss.sqlite3"
        journal = _DeliveryJournal(self._config(path))
        original_sequence = journal._connection.execute(
            """
            SELECT value FROM journal_metadata
            WHERE key = 'message_sequence'
            """
        ).fetchone()[0]
        transaction = journal._transaction()
        connection = transaction.__enter__()
        connection.execute(
            """
            UPDATE journal_metadata SET value = '999'
            WHERE key = 'message_sequence'
            """
        )
        holder = journal._owner_lock._process
        holder.kill()
        holder.wait(timeout=1.0)

        with self.assertRaisesRegex(_JournalError, "ownership was lost"):
            transaction.__exit__(None, None, None)
        self.assertFalse(connection.in_transaction)
        journal.close()

        reopened = _DeliveryJournal(self._config(path))
        try:
            recovered_sequence = reopened._connection.execute(
                """
                SELECT value FROM journal_metadata
                WHERE key = 'message_sequence'
                """
            ).fetchone()[0]
            self.assertEqual(recovered_sequence, original_sequence)
        finally:
            reopened.close()

    def test_corrupt_startup_reaps_partial_holder_and_closes_fds(self) -> None:
        path = self._root / "corrupt.sqlite3"
        path.write_bytes(b"not a sqlite database")
        baseline_fds = _fd_count()
        with self.assertRaises(_JournalError):
            _DeliveryJournal(self._config(path))
        if _holder_process_ids(path) is not None:
            self.assertTrue(
                _wait_for(
                    lambda: _holder_process_ids(path) == frozenset(),
                    timeout=1.0,
                )
            )
        if baseline_fds is not None:
            self.assertTrue(
                _wait_for(
                    lambda: _fd_count() is not None
                    and _fd_count() <= baseline_fds,
                    timeout=1.0,
                )
            )

    def test_holder_death_fails_endpoint_and_wakes_blocked_operations(
        self,
    ) -> None:
        path = self._root / "holder-death.sqlite3"
        baseline_fds = _fd_count()
        transport = _disconnected_transport("holder-death")
        delivery = DurableDelivery(transport, self._config(path))
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"value"),
            message_id="holder-death",
        )
        results: Queue[BaseException] = Queue()
        receive_started = Event()
        flush_started = Event()
        receive = Thread(
            target=_capture_blocked_receive,
            args=(delivery, receive_started, results),
        )
        flush = Thread(
            target=_capture_blocked_flush,
            args=(delivery, flush_started, results),
        )
        receive.start()
        flush.start()
        self.assertTrue(receive_started.wait(timeout=1.0))
        self.assertTrue(flush_started.wait(timeout=1.0))
        self.assertTrue(
            _wait_for(
                lambda: delivery._runtime._active_operations == 2,
                timeout=1.0,
            )
        )
        holder = delivery._journal._owner_lock._process
        holder.kill()
        holder.wait(timeout=1.0)
        delivery._sender.wake()

        receive.join(timeout=1.0)
        flush.join(timeout=1.0)
        self.assertFalse(receive.is_alive())
        self.assertFalse(flush.is_alive())
        failures = [results.get_nowait(), results.get_nowait()]
        self.assertTrue(
            all(isinstance(error, DeliveryClosed) for error in failures),
            failures,
        )
        health = delivery.health()
        self.assertTrue(health.closed)
        self.assertRegex(
            health.last_error or "",
            "owner lock exited unexpectedly",
        )
        with self.assertRaisesRegex(DeliveryError, "ownership was lost"):
            delivery.close()
        delivery.close()
        transport.close()
        self.assertIsNotNone(holder.poll())
        if baseline_fds is not None:
            self.assertTrue(
                _wait_for(
                    lambda: _fd_count() is not None
                    and _fd_count() <= baseline_fds,
                    timeout=1.0,
                )
            )

    def _config(self, path: Path) -> DeliveryConfig:
        return DeliveryConfig(
            path,
            max_outbox_items=4,
            max_inbox_items=4,
            max_storage_bytes=1024 * 1024,
            recovery_batch_size=4,
            max_message_bytes=4096,
            message_ttl_seconds=5.0,
            topic_policies=(
                TopicDeliveryPolicy.commands(
                    "events",
                    max_items=4,
                    max_bytes=1024 * 1024,
                    ttl_seconds=5.0,
                ),
            ),
        )


_SECOND_OWNER_PROBE = """
import sys
from pathlib import Path
from manyfold.architecture._transport_delivery_journal import _DeliveryJournal
from manyfold.architecture._transport_delivery_journal_errors import _JournalError
from manyfold.architecture._transport_delivery_policy import DeliveryConfig, TopicDeliveryPolicy

path = Path(sys.argv[1])
policy = TopicDeliveryPolicy.commands(
    "events", max_items=4, max_bytes=1024 * 1024, ttl_seconds=5.0
)
config = DeliveryConfig(
    path,
    max_outbox_items=4,
    max_inbox_items=4,
    max_storage_bytes=1024 * 1024,
    recovery_batch_size=4,
    message_ttl_seconds=5.0,
    topic_policies=(policy,),
)
try:
    owner = _DeliveryJournal(config)
except _JournalError:
    raise SystemExit(0)
owner.close()
print("SECOND_OWNER_DIFFERENT_TMPDIR")
raise SystemExit(4)
"""

_CRASH_OWNER = """
import os
import sys
from pathlib import Path
from manyfold.architecture._transport_delivery_journal import _DeliveryJournal
from manyfold.architecture._transport_delivery_policy import DeliveryConfig, TopicDeliveryPolicy

path = Path(sys.argv[1])
policy = TopicDeliveryPolicy.commands(
    "events", max_items=4, max_bytes=1024 * 1024, ttl_seconds=5.0
)
config = DeliveryConfig(
    path,
    max_outbox_items=4,
    max_inbox_items=4,
    max_storage_bytes=1024 * 1024,
    recovery_batch_size=4,
    message_ttl_seconds=5.0,
    topic_policies=(policy,),
)
owner = _DeliveryJournal(config)
print(owner._owner_lock._process.pid, flush=True)
os._exit(0)
"""

_CONCURRENT_OWNER = """
import sys
from pathlib import Path
from manyfold.architecture._transport_delivery_journal import _DeliveryJournal
from manyfold.architecture._transport_delivery_journal_errors import _JournalError
from manyfold.architecture._transport_delivery_policy import DeliveryConfig, TopicDeliveryPolicy

path = Path(sys.argv[1])
policy = TopicDeliveryPolicy.commands(
    "events", max_items=4, max_bytes=1024 * 1024, ttl_seconds=5.0
)
config = DeliveryConfig(
    path,
    max_outbox_items=4,
    max_inbox_items=4,
    max_storage_bytes=1024 * 1024,
    recovery_batch_size=4,
    message_ttl_seconds=5.0,
    topic_policies=(policy,),
)
print("ARMED", flush=True)
sys.stdin.read(1)
try:
    owner = _DeliveryJournal(config)
except _JournalError:
    print("OWNED", flush=True)
    raise SystemExit(0)
print("READY", flush=True)
sys.stdin.read(1)
owner.close()
"""


def _read_process_status(
    process: subprocess.Popen[str],
    results: Queue[tuple[subprocess.Popen[str], str]],
) -> None:
    if process.stdout is None:
        results.put((process, "NO_STDOUT"))
        return
    results.put((process, process.stdout.readline().strip()))


def _capture_blocked_receive(
    delivery: DurableDelivery,
    started: Event,
    results: Queue[BaseException],
) -> None:
    started.set()
    try:
        delivery.receive()
    except BaseException as error:
        results.put(error)


def _capture_blocked_flush(
    delivery: DurableDelivery,
    started: Event,
    results: Queue[BaseException],
) -> None:
    started.set()
    try:
        delivery.flush()
    except BaseException as error:
        results.put(error)


def _disconnected_transport(node_id: str) -> TcpTransport:
    return TcpTransport.connect(
        NodeIdentity("cluster", node_id),
        TcpAddress("127.0.0.1", 9),
        config=TransportConfig(
            security=TransportSecurity.insecure_local_development(),
            outbound_queue_limit=8,
            inbound_queue_limit=8,
            max_payload_bytes=64 * 1024,
            connect_timeout=0.05,
            handshake_timeout=0.1,
            heartbeat_interval=0.05,
            peer_timeout=0.2,
            reconnect=ReconnectPolicy(0.01, 2.0, 0.05),
        ),
        expected_peer_node_id="missing",
    )


def _fd_count() -> int | None:
    try:
        return len(os.listdir("/dev/fd"))
    except OSError:
        return None


def _holder_process_ids(journal_path: Path) -> frozenset[int] | None:
    module = "manyfold.architecture._transport_delivery_lock_holder"
    target = str(journal_path)
    if os.name == "nt":
        escaped_target = target.replace("'", "''")
        command = (
            "Get-CimInstance Win32_Process | "
            "Where-Object { "
            "$_.Name -like 'python*' -and "
            f"$_.CommandLine -like '*{module}*' -and "
            f"$_.CommandLine -like '*{escaped_target}*' "
            "} | "
            "Select-Object -ExpandProperty ProcessId"
        )
        try:
            result = subprocess.run(
                ["powershell", "-NoProfile", "-Command", command],
                check=False,
                capture_output=True,
                text=True,
                timeout=5.0,
            )
        except (OSError, subprocess.TimeoutExpired):
            return None
        if result.returncode != 0:
            return None
        return frozenset(
            int(line.strip())
            for line in result.stdout.splitlines()
            if line.strip().isdigit()
        )
    try:
        result = subprocess.run(
            ["ps", "-axo", "pid=,command="],
            check=False,
            capture_output=True,
            text=True,
            timeout=5.0,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    return frozenset(
        int(line.split(maxsplit=1)[0])
        for line in result.stdout.splitlines()
        if (
            module in line
            and target in line
            and line.split(maxsplit=1)[0].isdigit()
        )
    )


def _wait_for(predicate: object, *, timeout: float) -> bool:
    if not callable(predicate):
        raise TypeError("predicate must be callable")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return bool(predicate())


def _external_holder_exited(pid: int, journal_path: Path) -> bool:
    if os.name == "nt":
        holder_ids = _holder_process_ids(journal_path)
        return holder_ids is not None and pid not in holder_ids
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


if __name__ == "__main__":
    unittest.main()
