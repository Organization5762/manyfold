from __future__ import annotations

import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from threading import get_ident

from manyfold.architecture import (
    ImmutableValue,
    StateMachine,
    StateMachineAlreadyRunning,
    StateMachineEvent,
    StateMachineEventType,
    StateRejection,
    StateTransition,
    StateUnchanged,
)


@dataclass(frozen=True)
class CounterCommand:
    delta: int
    mode: str


@dataclass(frozen=True)
class CounterState:
    total: int


@dataclass(frozen=True)
class CounterTransition:
    previous: int
    current: int


class ArchitectureStateMachineTests(unittest.TestCase):
    def setUp(self) -> None:
        self._machines: list[
            StateMachine[CounterCommand, CounterState, CounterTransition]
        ] = []

    def tearDown(self) -> None:
        for machine in reversed(self._machines):
            machine.close()

    def test_valid_transition_has_typed_surfaces_and_semantic_golden_trace(
        self,
    ) -> None:
        machine = self._machine("golden")
        observed: list[StateMachineEvent] = []
        machine.events.subscribe(
            lambda row: observed.append(row.as_model(StateMachineEvent))
        )

        machine.start()
        machine.commands.publish(
            CounterCommand(delta=3, mode="commit"),
            key="command-7",
        )
        self.assertTrue(machine.flush(timeout=1.0))
        machine.close()

        self.assertIsInstance(machine.state, ImmutableValue)
        self.assertFalse(hasattr(machine.state, "set"))
        self.assertEqual(machine.state.latest, CounterState(total=3))
        self.assertEqual(
            machine.states.latest().as_model(CounterState),
            CounterState(total=3),
        )
        self.assertEqual(
            machine.transitions.latest().as_model(CounterTransition),
            CounterTransition(previous=0, current=3),
        )
        self.assertEqual(
            [
                (
                    event.kind,
                    event.sequence,
                    event.revision,
                    event.command_id,
                    event.correlation_id,
                    event.reason,
                )
                for event in observed
            ],
            [
                ("starting", 1, 0, "", "", "start"),
                ("started", 2, 0, "", "", "ready"),
                (
                    "command_received",
                    3,
                    0,
                    "test.golden:3",
                    "command-7",
                    "received",
                ),
                (
                    "committed",
                    4,
                    1,
                    "test.golden:3",
                    "command-7",
                    "counter_changed",
                ),
                ("stopping", 5, 1, "", "", "close"),
                ("stopped", 6, 1, "", "", "closed"),
            ],
        )
        command_received = observed[2]
        committed = observed[3]
        self.assertIs(
            committed.event_type,
            StateMachineEventType.COMMITTED,
        )
        self.assertEqual(
            command_received.previous_state_id,
            command_received.next_state_id,
        )
        self.assertNotEqual(
            committed.previous_state_id,
            committed.next_state_id,
        )

    def test_rejected_and_unchanged_commands_do_not_advance_revision(self) -> None:
        machine = self._machine("outcomes")
        machine.start()

        machine.commands.publish(
            CounterCommand(delta=1, mode="reject"),
            key="rejected",
        )
        machine.commands.publish(
            CounterCommand(delta=1, mode="unchanged"),
            key="unchanged",
        )
        self.assertTrue(machine.flush(timeout=1.0))

        events = _audit_events(machine)
        outcomes = [
            event
            for event in events
            if event.kind
            in {
                StateMachineEventType.REJECTED.value,
                StateMachineEventType.UNCHANGED.value,
            }
        ]
        self.assertEqual(
            [(event.kind, event.revision, event.reason) for event in outcomes],
            [
                ("rejected", 0, "command_rejected"),
                ("unchanged", 0, "already_current"),
            ],
        )
        self.assertEqual(machine.state.latest, CounterState(total=0))
        self.assertEqual(machine.revision, 0)

    def test_concurrent_publishers_are_serialized_without_lost_updates(self) -> None:
        machine = self._machine("concurrent", queue_limit=128)
        machine.start()

        with ThreadPoolExecutor(max_workers=8) as executor:
            publications = [
                executor.submit(
                    machine.commands.publish,
                    CounterCommand(delta=1, mode="commit"),
                    key=f"command-{index}",
                )
                for index in range(64)
            ]
            for publication in publications:
                publication.result()

        self.assertTrue(machine.flush(timeout=2.0))
        self.assertEqual(machine.state.latest, CounterState(total=64))
        self.assertEqual(machine.revision, 64)
        committed = [
            event
            for event in _audit_events(machine)
            if event.kind == StateMachineEventType.COMMITTED.value
        ]
        self.assertEqual(len(committed), 64)
        self.assertEqual(
            [event.revision for event in committed],
            list(range(1, 65)),
        )

    def test_subscriber_failure_isolated_from_later_commands(self) -> None:
        machine = self._machine("subscriber-failure")
        callback_threads: list[int] = []

        def fail_transition(_row: object) -> None:
            callback_threads.append(get_ident())
            raise RuntimeError("broken transition consumer")

        machine.transitions.subscribe(fail_transition)
        machine.start()
        caller_thread = get_ident()

        machine.commands.publish(CounterCommand(delta=1, mode="commit"))
        machine.commands.publish(CounterCommand(delta=1, mode="commit"))
        self.assertTrue(machine.flush(timeout=1.0))

        self.assertEqual(machine.state.latest, CounterState(total=2))
        self.assertEqual(machine.revision, 2)
        self.assertTrue(callback_threads)
        self.assertTrue(all(thread != caller_thread for thread in callback_threads))
        failures = [
            event
            for event in _audit_events(machine)
            if event.kind == StateMachineEventType.FAILED.value
        ]
        self.assertEqual(len(failures), 2)
        self.assertTrue(
            all("broken transition consumer" in event.reason for event in failures)
        )
        self.assertIn("broken transition consumer", machine.last_error)

    def test_reducer_failure_is_published_and_worker_stays_live(self) -> None:
        machine = self._machine("reducer-failure")
        machine.start()

        machine.commands.publish(CounterCommand(delta=1, mode="fail"))
        machine.commands.publish(CounterCommand(delta=2, mode="commit"))
        self.assertTrue(machine.flush(timeout=1.0))

        self.assertEqual(machine.state.latest, CounterState(total=2))
        self.assertEqual(machine.revision, 1)
        failures = [
            event
            for event in _audit_events(machine)
            if event.kind == StateMachineEventType.FAILED.value
        ]
        self.assertEqual(len(failures), 1)
        self.assertIn("reducer exploded", failures[0].reason)

    def test_close_drains_accepted_work_and_drops_future_processing(self) -> None:
        machine = self._machine("close")
        machine.start()
        machine.commands.publish(CounterCommand(delta=4, mode="commit"))

        machine.close(timeout=1.0)
        machine.commands.publish(CounterCommand(delta=9, mode="commit"))

        self.assertFalse(machine.is_running)
        self.assertEqual(machine.state.latest, CounterState(total=4))
        self.assertEqual(machine.revision, 1)
        self.assertEqual(
            [event.kind for event in _audit_events(machine)][-2:],
            ["stopping", "stopped"],
        )
        self.assertEqual(
            machine.commands.latest().as_model(CounterCommand),
            CounterCommand(delta=9, mode="commit"),
        )

    def test_duplicate_active_name_is_rejected_and_close_releases_it(self) -> None:
        first = self._machine("identity")
        second = self._machine("identity")
        first.start()

        with self.assertRaisesRegex(
            StateMachineAlreadyRunning,
            "already running",
        ):
            second.start()

        first.close()
        second.start()
        self.assertTrue(second.is_running)

    def test_in_process_restart_restores_retained_state_and_revision(self) -> None:
        first = self._machine("restart", initial=CounterState(total=0))
        first.start()
        first.commands.publish(
            CounterCommand(delta=5, mode="commit"),
            key="before-restart",
        )
        self.assertTrue(first.flush(timeout=1.0))
        first.close()

        restarted = self._machine("restart", initial=CounterState(total=99))
        restarted.start()

        self.assertEqual(restarted.state.latest, CounterState(total=5))
        self.assertEqual(restarted.revision, 1)
        self.assertGreaterEqual(
            restarted.events.latest().sequence,
            8,
        )
        restarted.commands.publish(
            CounterCommand(delta=2, mode="commit"),
            key="after-restart",
        )
        self.assertTrue(restarted.flush(timeout=1.0))
        self.assertEqual(restarted.state.latest, CounterState(total=7))
        self.assertEqual(restarted.revision, 2)

    def test_contracts_reject_invalid_configuration_and_results(self) -> None:
        with self.assertRaisesRegex(ValueError, "state-machine name"):
            StateMachine(
                name="",
                initial=CounterState(0),
                reducer=_reduce_counter,
                command_schema=CounterCommand,
                state_schema=CounterState,
                event_schema=CounterTransition,
            )
        with self.assertRaisesRegex(TypeError, "initial state"):
            StateMachine(
                name="wrong-initial",
                initial=CounterCommand(1, "commit"),  # type: ignore[arg-type]
                reducer=_reduce_counter,
                command_schema=CounterCommand,
                state_schema=CounterState,
                event_schema=CounterTransition,
            )

    def _machine(
        self,
        suffix: str,
        *,
        initial: CounterState = CounterState(total=0),
        queue_limit: int = 16,
    ) -> StateMachine[CounterCommand, CounterState, CounterTransition]:
        machine = StateMachine(
            name=f"test.{suffix}",
            initial=initial,
            reducer=_reduce_counter,
            command_schema=CounterCommand,
            state_schema=CounterState,
            event_schema=CounterTransition,
            namespace=f"state-machine-tests.{self._testMethodName}",
            command_queue_limit=queue_limit,
            retained_messages=512,
        )
        self._machines.append(machine)
        return machine


def _reduce_counter(
    state: CounterState,
    command: CounterCommand,
) -> (
    StateTransition[CounterState, CounterTransition]
    | StateRejection
    | StateUnchanged
):
    if command.mode == "reject":
        return StateRejection("command_rejected")
    if command.mode == "unchanged":
        return StateUnchanged("already_current")
    if command.mode == "fail":
        raise RuntimeError("reducer exploded")
    next_state = CounterState(total=state.total + command.delta)
    return StateTransition(
        next_state,
        events=(
            CounterTransition(
                previous=state.total,
                current=next_state.total,
            ),
        ),
        reason="counter_changed",
    )


def _audit_events(
    machine: StateMachine[CounterCommand, CounterState, CounterTransition],
) -> list[StateMachineEvent]:
    return [
        row.as_model(StateMachineEvent)
        for row in machine.events.query(
            """
            SELECT *
            FROM stream
            ORDER BY sequence
            """
        )
    ]


if __name__ == "__main__":
    unittest.main()
