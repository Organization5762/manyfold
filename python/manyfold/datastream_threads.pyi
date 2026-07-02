from collections.abc import Callable, Iterable, Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import timedelta
from enum import IntEnum
from threading import Thread
from typing import Any, TypeVar

from .streams import Observable, SchedulerBase, Subject, TimeoutScheduler

T = TypeVar("T")
TStarting = TypeVar("TStarting")
StartableTarget = Callable[..., None]
DataStreamPriorityResolver = Callable[[], "DataStreamPriority"]

MAIN_THREAD_DATASTREAM_LATENCY: str
DEFAULT_BACKGROUND_PRIORITY_QUEUE_LIMIT: int
DEFAULT_DELIVERY_LATENCY_HISTORY_SIZE: int
DEFAULT_MAIN_THREAD_DATASTREAM_QUEUE_LIMIT: int
shutdown_signal: Subject[Any]

class DataStreamPriority(IntEnum):
    HIGH = 0
    NORMAL = 10
    LOW = 20

@dataclass(frozen=True, slots=True)
class DeliveryLatencyStats:
    count: int
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float

def background_scheduler() -> SchedulerBase: ...
def blocking_io_scheduler() -> SchedulerBase: ...
def input_scheduler() -> SchedulerBase: ...
def interval_scheduler() -> SchedulerBase: ...
def coalesce_scheduler() -> SchedulerBase: ...
def replay_scheduler() -> TimeoutScheduler: ...
def create_default_thread_factory(name: str) -> Callable[[StartableTarget], Thread]: ...
def interval_in_background(
    period: timedelta,
    *,
    name: str | None = None,
    scheduler: SchedulerBase | None = None,
) -> Observable[int]: ...
def delivery_latency_snapshot() -> dict[str, DeliveryLatencyStats]: ...
def drain_main_thread_queue(max_items: int | None = None) -> int: ...
def on_main_thread() -> bool: ...
def deliver_on_main_thread(source: Observable[T]) -> Observable[T]: ...
def deliver_on_background(
    source: Observable[T],
    *,
    priority: DataStreamPriority | DataStreamPriorityResolver = ...,
) -> Observable[T]: ...
def start_with_once(value: TStarting) -> Callable[[Observable[T]], Observable[Any]]: ...
def pipe_on_background(
    source: Observable[T],
    *operators: Any,
    starting_value: TStarting | object = ...,
) -> Observable[Any]: ...
def pipe_on_main_thread(source: Observable[T], *operators: Any) -> Observable[Any]: ...
def pipe_to_background_event_loop(
    source: Observable[T],
    name: str,
    *operators: Any,
) -> Observable[Any]: ...
def pipe_to_background_thread(
    source: Observable[T],
    name: str,
    *operators: Any,
) -> Observable[Any]: ...
def background_datastream_observable(
    observable: Observable[T],
    name: str,
) -> AbstractContextManager[Iterator[Observable[T]]]: ...
def scheduler_diagnostics() -> dict[str, int | None]: ...
def reset_datastream_delivery_for_tests() -> None: ...
def materialize_sequence(sequence: Iterable[T]) -> Observable[T]: ...
