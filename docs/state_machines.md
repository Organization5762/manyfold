# PubSub state machines

`manyfold.architecture.state_machines` turns a pure reducer into one
single-writer state machine backed by existing named PubSub topics and
architecture values. It adds no observable operators, transport, journal, or
retry loop.

```python
from dataclasses import dataclass

from manyfold.architecture import (
    StateMachine,
    StateTransition,
)


@dataclass(frozen=True)
class NavigationCommand:
    scene: str


@dataclass(frozen=True)
class NavigationState:
    scene: str


@dataclass(frozen=True)
class NavigationTransition:
    scene: str


def reduce_navigation(
    state: NavigationState,
    command: NavigationCommand,
) -> StateTransition[NavigationState, NavigationTransition]:
    next_state = NavigationState(scene=command.scene)
    return StateTransition(
        next_state,
        events=(NavigationTransition(scene=command.scene),),
        reason="scene_selected",
    )


machine = StateMachine(
    name="heart.navigation",
    initial=NavigationState(scene="home"),
    reducer=reduce_navigation,
    command_schema=NavigationCommand,
    state_schema=NavigationState,
    event_schema=NavigationTransition,
)
machine.start()
try:
    machine.commands.publish(
        NavigationCommand(scene="water-cube"),
        key="navigation-7",
    )
    machine.flush(timeout=1.0)
    print(machine.state.latest)
finally:
    machine.close()
```

Sample output:

```text
NavigationState(scene='water-cube')
```

## Public model

Each machine owns four real `PubSubTopic` handles in its selected namespace:

| Surface | Topic | Meaning |
| --- | --- | --- |
| `commands` | `<name>.commands` | Typed domain commands accepted by the reducer. |
| `states` | `<name>.states` | Typed committed state snapshots, including the initial state. |
| `transitions` | `<name>.transitions` | Typed domain events returned by committed transitions. |
| `events` | `<name>.events` | `StateMachineEvent` lifecycle, command, commit, rejection, and failure records. |

`state` is a live read-only `ImmutableValue` view. The reducer is the only
writer. `start()` publishes deterministic `starting` and `started` audit events;
`close()` stops command admission, drains accepted work, publishes `stopping`
and `stopped`, and disposes the command subscription.

Reducers return one explicit result:

- `StateTransition(next_state, events=..., reason=...)` commits a new revision.
- `StateRejection(reason)` records a rejected command without changing state.
- `StateUnchanged(reason)` records an accepted no-transition command.

`StateMachineEvent.event_type` exposes the typed `StateMachineEventType`;
`sequence` strictly orders semantic audit records at their publication
boundary. Use the sequence, rather than a timestamp or storage offset, when
normalizing golden traces.
`StateMachineEvent.revision` starts at zero and increases only for committed
state changes. Command IDs default to `<machine>:<fabric-offset>`; a command
`key=` becomes its correlation ID. State identities are deterministic hashes of
typed field values, so semantic golden traces do not depend on time or random
UUIDs.

## Concurrency and callbacks

The command-topic callback only places the decoded command in a bounded queue.
One machine-owned thread invokes the reducer and publishes the resulting state
and events in queue order. Concurrent command publishers therefore cannot lose
updates.

Reducer, state-value, state-topic, and transition-topic callbacks for command
processing run on the machine thread. Lifecycle event callbacks from `start()`
and `close()` run on the calling thread. PubSub subscribers otherwise retain
their configured callback placement. No state or audit lock is held while the
reducer, state-value observers, or domain-topic subscribers run.

A reducer or state/domain subscriber failure becomes a `failed` audit record;
it does not stop later commands. An audit-event subscriber failure cannot
recursively publish another audit event, so it is exposed through
`machine.last_error` instead. As with existing PubSub behavior, one failing
synchronous topic callback may prevent later callbacks for that publication.
`flush()` is the deterministic test and shutdown barrier.

## Restart, replay, and durability

Named PubSub fabrics are process-local in the current runtime. Recreating a
closed machine with the same name and namespace restores the latest retained
state and audit revision in that process. It does not provide process-restart
durability.

Commands accepted while a machine is stopped are not replayed automatically.
Current PubSub and durable delivery cannot atomically commit all of:

1. durable command consumption;
2. the next state and revision;
3. transition and audit events;
4. the processed command ID.

Automatic replay would therefore imply exactly-once behavior that the current
storage boundaries cannot guarantee. Applications may use stable command
`key=` values for correlation, but deduplication remains the durable consumer's
responsibility until an atomic state-machine commit boundary exists.

The topics are directly bindable by the durable mesh work without an adapter:

- commands: bounded append/deduplicated delivery;
- state snapshots: latest per machine;
- domain transitions: bounded append delivery;
- semantic audit events: bounded append delivery.

Lifecycle telemetry from the transport remains separate from domain audit
events. This state-machine module does not own transport receive loops, ACKs,
retries, journals, or Raft.

## Heart migration

Heart can replace an ad hoc navigation lock, mutable current-scene field,
callback fanout, and manually assembled transition log with one reducer:

```python
navigation.commands.publish(
    NavigationCommand(scene="water-cube"),
    key=request_id,
)
```

Scene selection policy moves into `reduce_navigation`; readers use
`navigation.state.latest`; render or persistence consumers subscribe to
`navigation.transitions`; qualification compares the stable semantic fields on
`navigation.events`. Device adapters and rendering remain outside the reducer.
