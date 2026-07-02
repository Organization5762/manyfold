from __future__ import annotations

from collections.abc import Callable
from typing import Generic, TypeVar

T = TypeVar("T")

class ValueSubscription:
    @property
    def is_disposed(self) -> bool: ...
    def dispose(self) -> bool: ...
    def __enter__(self) -> ValueSubscription: ...
    def __exit__(self, *_exc: object) -> None: ...

class NewValues(Generic[T]):
    name: str | None
    def __init__(self, *, name: str | None = None) -> None: ...
    @property
    def subscriber_count(self) -> int: ...
    def publish(self, value: T) -> None: ...
    def set(self, value: T) -> None: ...
    def emit(self, value: T) -> None: ...
    def observe(
        self,
        callback: Callable[[T], object] | object | None = None,
        *,
        on_next: Callable[[T], object] | None = None,
    ) -> ValueSubscription: ...
    def subscribe(
        self,
        callback: Callable[[T], object] | object | None = None,
        on_error: Callable[[Exception], object] | None = None,
        on_completed: Callable[[], object] | None = None,
        scheduler: object | None = None,
        *,
        on_next: Callable[[T], object] | None = None,
    ) -> ValueSubscription: ...
    def from_stream(
        self,
        stream: object | None = None,
        *,
        namespace: str = "default",
        schema: type | None = None,
        retained_messages: int = 1024,
    ) -> object: ...

class ImmutableValue(Generic[T]):
    name: str | None
    def __init__(
        self,
        initial: T | None = None,
        *,
        name: str | None = None,
        has_initial: bool = False,
    ) -> None: ...
    @property
    def has_value(self) -> bool: ...
    @property
    def latest(self) -> T | None: ...
    @property
    def subscriber_count(self) -> int: ...
    @classmethod
    def initialized(
        cls,
        value: T,
        *,
        name: str | None = None,
    ) -> ImmutableValue[T]: ...
    def observe(
        self,
        callback: Callable[[T], object] | object | None = None,
        *,
        on_next: Callable[[T], object] | None = None,
        replay_latest: bool = True,
    ) -> ValueSubscription: ...
    def subscribe(
        self,
        callback: Callable[[T], object] | object | None = None,
        on_error: Callable[[Exception], object] | None = None,
        on_completed: Callable[[], object] | None = None,
        scheduler: object | None = None,
        *,
        on_next: Callable[[T], object] | None = None,
        replay_latest: bool = True,
    ) -> ValueSubscription: ...
    def map(self, transform: Callable[[T], object]) -> object: ...

class Value(ImmutableValue[T]):
    def __init__(
        self,
        initial: T | None = None,
        *,
        name: str | None = None,
        has_initial: bool = False,
    ) -> None: ...
    @property
    def subscriber_count(self) -> int: ...
    @classmethod
    def initialized(cls, value: T, *, name: str | None = None) -> Value[T]: ...
    def set(self, value: T) -> None: ...
    def observe(
        self,
        callback: Callable[[T], object] | object | None = None,
        *,
        on_next: Callable[[T], object] | None = None,
        replay_latest: bool = True,
    ) -> ValueSubscription: ...

class HistoricalValue(Generic[T]):
    name: str | None
    retained_values: int
    def __init__(
        self,
        *,
        name: str | None = None,
        retained_values: int = 1024,
    ) -> None: ...
    @property
    def subscriber_count(self) -> int: ...
    @property
    def latest(self) -> T | None: ...
    def append(self, value: T) -> None: ...
    def publish(self, value: T) -> None: ...
    def set(self, value: T) -> None: ...
    def replay(self, *, limit: int | None = None) -> tuple[T, ...]: ...
    def observe(
        self,
        callback: Callable[[T], object] | object | None = None,
        *,
        on_next: Callable[[T], object] | None = None,
        replay: bool | int = False,
    ) -> ValueSubscription: ...
    def subscribe(
        self,
        callback: Callable[[T], object] | object | None = None,
        on_error: Callable[[Exception], object] | None = None,
        on_completed: Callable[[], object] | None = None,
        scheduler: object | None = None,
        *,
        on_next: Callable[[T], object] | None = None,
        replay: bool | int = False,
    ) -> ValueSubscription: ...
    def from_stream(
        self,
        stream: object | None = None,
        *,
        namespace: str = "default",
        schema: type | None = None,
        retained_messages: int | None = None,
    ) -> object: ...

__all__: list[str]
