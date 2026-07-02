from collections.abc import Callable
from datetime import timedelta
from enum import IntEnum
from threading import Thread
from typing import TypeVar

from .streams import Observable, SchedulerBase

T = TypeVar("T")
StartableTarget = Callable[..., None]
DataStreamPriorityResolver = Callable[[], "DataStreamPriority"]

DEFAULT_BACKGROUND_PRIORITY_QUEUE_LIMIT: int
DEFAULT_MAIN_THREAD_DATASTREAM_QUEUE_LIMIT: int

class DataStreamPriority(IntEnum):
    HIGH = 0
    NORMAL = 10
    LOW = 20

def background_scheduler() -> SchedulerBase: ...
def blocking_io_scheduler() -> SchedulerBase: ...
def input_scheduler() -> SchedulerBase: ...
def interval_scheduler() -> SchedulerBase: ...
def coalesce_scheduler() -> SchedulerBase: ...
def create_default_thread_factory(name: str) -> Callable[[StartableTarget], Thread]: ...
def interval_in_background(
    period: timedelta,
    *,
    name: str | None = None,
    scheduler: SchedulerBase | None = None,
) -> Observable[int]: ...
def drain_main_thread_queue(max_items: int | None = None) -> int: ...
def on_main_thread() -> bool: ...
def deliver_on_main_thread(source: Observable[T]) -> Observable[T]: ...
def deliver_on_background(
    source: Observable[T],
    *,
    priority: DataStreamPriority | DataStreamPriorityResolver | None = ...,
) -> Observable[T]: ...
def reset_datastream_delivery_for_tests() -> None: ...
