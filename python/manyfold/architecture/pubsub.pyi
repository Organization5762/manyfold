from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from subprocess import Popen

from manyfold._manyfold_rust import (
    InMemoryPubSub as InMemoryPubSub,
    PubSubDelivery as PubSubDelivery,
    PubSubMessage as PubSubMessage,
    PubSubSubscription as PubSubSubscription,
)
from manyfold.architecture.callbacks import CallbackPlacement
from manyfold.architecture.locks import Lock

class StreamRow(Mapping[str, object]):
    def __getitem__(self, name: str) -> object: ...
    def __iter__(self) -> Iterator[str]: ...
    def __len__(self) -> int: ...
    def __getattr__(self, name: str) -> object: ...
    def as_dict(self) -> dict[str, object]: ...
    def as_model(self, model: type) -> object: ...

class PubSub:
    topic: str
    schema: type | object | None
    schedule: PubSubSchedule | None
    @staticmethod
    def spawn_rust_worker(
        command: str,
        args: tuple[str, ...] = (),
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> Popen[str]: ...
    @property
    def value(self) -> PubSubValueSurface: ...
    def lock(self) -> Lock: ...
    def clock(self) -> object: ...
    def __init__(
        self,
        *,
        topic: str | None = None,
        schema: type | object | None = None,
        retained_messages: int = 1024,
        schedule: bool = True,
        worker_name: str | None = None,
        service_discovery: bool = True,
    ) -> None: ...
    def publish(
        self,
        payload: object | None = None,
        *,
        event_time: int | None = None,
        key: str | None = None,
        **fields: object,
    ) -> None: ...
    def callback(
        self,
        callback: Callable[[StreamRow], object],
        *,
        callback_placement: CallbackPlacement | None = None,
        name: str | None = None,
        replay_latest: bool = False,
    ) -> PubSubCallbackSubscription: ...
    def pipe(
        self,
        *operators: Callable[[PubSubObservable], PubSubObservable],
    ) -> PubSubObservable: ...
    def start_with(self, *values: object) -> PubSubObservable: ...
    def subscribe(
        self,
        callback: Callable[[StreamRow], object] | object | None = None,
        on_error: Callable[[Exception], object] | None = None,
        on_completed: Callable[[], object] | None = None,
        scheduler: object | None = None,
        *,
        callback_placement: CallbackPlacement | None = None,
        on_next: Callable[[StreamRow], object] | None = None,
        replay_latest: bool = False,
    ) -> PubSubCallbackSubscription: ...
    def map(
        self,
        transform: Callable[[StreamRow], object],
        *,
        name: str | None = None,
    ) -> PubSubObservable: ...
    def filter(
        self,
        predicate: Callable[[StreamRow], bool],
        *,
        name: str | None = None,
    ) -> PubSubObservable: ...
    def scan(
        self,
        reducer: Callable[[object, StreamRow], object],
        initial: object = ...,
        *,
        seed: object = ...,
    ) -> PubSubObservable: ...
    def with_latest_from(self, *others: object) -> PubSubObservable: ...
    def combine_latest(self, *others: object) -> PubSubObservable: ...
    def deliver_on(self, placement: CallbackPlacement) -> PubSubObservable: ...
    def state(
        self,
        initial: object,
        reducer: Callable[[object, StreamRow], object],
    ) -> PubSubObservable: ...
    def do_action(
        self,
        action: Callable[[StreamRow], object] | None = None,
        *_args: object,
        on_next: Callable[[StreamRow], object] | None = None,
        **_kwargs: object,
    ) -> PubSubObservable: ...
    def flat_map(self, mapper: Callable[[StreamRow], object]) -> PubSubObservable: ...
    def switch_latest(
        self,
        mapper: Callable[[StreamRow], object] | None = None,
    ) -> PubSubObservable: ...
    def where(self, **field_values: object) -> list[StreamRow]: ...
    def project(self, *fields: str) -> list[StreamRow]: ...
    def take(self, count: int = 1) -> list[StreamRow]: ...
    def distinct_until_changed(
        self,
        field: str | None = None,
    ) -> list[StreamRow] | PubSubObservable: ...
    def pairwise(
        self, field: str | None = None
    ) -> list[StreamRow] | PubSubObservable: ...
    def latest_join(
        self,
        other: PubSub,
        *fields: str,
        prefix: str | None = None,
    ) -> list[StreamRow]: ...
    def recursive_sum(
        self,
        field: str,
        *,
        value: str = "elapsed_ms",
        initial: float = 0.0,
    ) -> list[StreamRow]: ...
    def query(
        self,
        sql: str,
        parameters: dict[str, object] | None = None,
    ) -> list[StreamRow]: ...
    def query_one(
        self,
        sql: str,
        parameters: dict[str, object] | None = None,
    ) -> StreamRow | None: ...
    def latest(self) -> StreamRow | None: ...
    def average(self, *, field: str) -> float | None: ...

class PubSubFabric:
    name: str
    namespace: str
    schedule: PubSubSchedule
    @property
    def topics(self) -> tuple[str, ...]: ...
    def __init__(
        self,
        *,
        namespace: str = "default",
        name: str | None = None,
        retained_messages: int = 1024,
        worker_name: str | None = None,
        service_discovery: bool = True,
    ) -> None: ...
    def topic(self, topic: str, *, schema: type | None = None) -> PubSub: ...

class PubSubValueSurface:
    @property
    def stream(self) -> PubSub: ...
    @property
    def current(self) -> PubSubCurrentValueSurface: ...
    def latest(self) -> object: ...
    def historical(self, *, retained_values: int = 1024) -> object: ...
    def history(self, retained_values: int) -> object: ...
    def new_values(self) -> object: ...

class PubSubCurrentValueSurface:
    @property
    def stream(self) -> PubSub: ...
    def latest(self) -> object: ...

def PubSubTopic(
    name: str | None = None,
    *,
    schema: type | None = None,
    namespace: str = "default",
    retained_messages: int = 1024,
    worker_name: str | None = None,
    service_discovery: bool = True,
    pubsub: str | None = None,
    fabric: str | None = None,
) -> PubSub: ...

class PubSubCallbackSubscription:
    @property
    def is_disposed(self) -> bool: ...
    def dispose(self) -> bool: ...

class PubSubObservable:
    @classmethod
    def merge(cls, *sources: object) -> PubSubObservable: ...
    def subscribe(
        self,
        callback: Callable[[object], object] | object | None = None,
        on_error: Callable[[Exception], object] | None = None,
        on_completed: Callable[[], object] | None = None,
        scheduler: object | None = None,
        *,
        callback_placement: CallbackPlacement | None = None,
        on_next: Callable[[object], object] | None = None,
        replay_latest: bool = False,
    ) -> PubSubCallbackSubscription: ...
    def callback(
        self,
        callback: Callable[[object], object],
        *,
        callback_placement: CallbackPlacement | None = None,
        name: str | None = None,
        replay_latest: bool = False,
    ) -> PubSubCallbackSubscription: ...
    def deliver_on(self, placement: CallbackPlacement) -> PubSubObservable: ...
    def pipe(
        self,
        *operators: Callable[[PubSubObservable], PubSubObservable],
    ) -> PubSubObservable: ...
    def start_with(self, *values: object) -> PubSubObservable: ...
    def map(
        self,
        transform: Callable[[object], object],
        *,
        name: str | None = None,
    ) -> PubSubObservable: ...
    def filter(
        self,
        predicate: Callable[[object], bool],
        *,
        name: str | None = None,
    ) -> PubSubObservable: ...
    def scan(
        self,
        reducer: Callable[[object, object], object],
        initial: object = ...,
        *,
        seed: object = ...,
    ) -> PubSubObservable: ...
    def with_latest_from(self, *others: object) -> PubSubObservable: ...
    def combine_latest(self, *others: object) -> PubSubObservable: ...
    def state(
        self,
        initial: object,
        reducer: Callable[[object, object], object],
    ) -> PubSubObservable: ...
    def do_action(
        self,
        action: Callable[[object], object] | None = None,
        *_args: object,
        on_next: Callable[[object], object] | None = None,
        **_kwargs: object,
    ) -> PubSubObservable: ...
    def distinct_until_changed(
        self,
        key: Callable[[object], object] | None = None,
    ) -> PubSubObservable: ...
    def pairwise(self) -> PubSubObservable: ...
    def take(self, count: int) -> PubSubObservable: ...
    def flat_map(self, mapper: Callable[[object], object]) -> PubSubObservable: ...
    def switch_latest(
        self,
        mapper: Callable[[object], object] | None = None,
    ) -> PubSubObservable: ...

class ServiceDiscoveryRequirement:
    service: str
    affinity: str
    spawn_policy: str
    address: str

class PubSubSchedule:
    worker: str
    affinity: str
    process_id: int
    thread_id: int
    address: str
    service_discovery: ServiceDiscoveryRequirement | None
    @classmethod
    def current_thread(
        cls,
        *,
        topic: str,
        worker_name: str | None = None,
        service_discovery: bool = True,
    ) -> PubSubSchedule: ...

__all__: list[str]
