from __future__ import annotations

from collections.abc import Iterator, Mapping

from manyfold._manyfold_rust import (
    InMemoryPubSub as InMemoryPubSub,
    PubSubDelivery as PubSubDelivery,
    PubSubMessage as PubSubMessage,
    PubSubSubscription as PubSubSubscription,
)

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
    def __init__(
        self,
        *,
        topic: str | None = None,
        schema: type | object | None = None,
        retained_messages: int = 1024,
    ) -> None: ...
    def publish(
        self,
        payload: object | None = None,
        *,
        event_time: int | None = None,
        key: str | None = None,
        **fields: object,
    ) -> None: ...
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

__all__: list[str]
