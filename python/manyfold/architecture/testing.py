"""Testing helpers for architecture-level ManyFold primitives."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from uuid import uuid4

from manyfold.architecture.pubsub import (
    PubSub,
    PubSubCallbackSubscription,
    PubSubFabric,
)


def pubsub_marbles(
    *,
    namespace: str | None = None,
    retained_messages: int = 1024,
) -> "PubSubMarbles":
    """Create a context-managed PubSub marble harness."""
    return PubSubMarbles(namespace=namespace, retained_messages=retained_messages)


@dataclass(frozen=True, slots=True)
class PubSubMarbleRecord:
    """One value observed at a virtual marble frame."""

    frame: int
    value: object


class PubSubMarbles:
    """Context-managed PubSub marble harness backed by a fresh Rust runtime."""

    def __init__(
        self,
        *,
        namespace: str | None = None,
        retained_messages: int = 1024,
    ) -> None:
        self.namespace = namespace or f"manyfold-test-{uuid4()}"
        self._fabric: PubSubFabric | None = PubSubFabric(
            namespace=self.namespace,
            retained_messages=retained_messages,
            service_discovery=False,
        )
        self._subscriptions: list[PubSubCallbackSubscription] = []
        self._closed = False

    def __enter__(self) -> "PubSubMarbles":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def topic(self, name: str, *, schema: type | None = None) -> PubSub:
        """Return a topic owned by this marble harness runtime."""
        self._ensure_open()
        if self._fabric is None:
            raise RuntimeError("PubSubMarbles is closed")
        return self._fabric.topic(name, schema=schema)

    def observe(
        self,
        source: object,
        *,
        value: Callable[[object], object] | None = None,
    ) -> list[PubSubMarbleRecord]:
        """Subscribe to ``source`` and return the mutable observed record list."""
        self._ensure_open()
        records: list[PubSubMarbleRecord] = []
        value_of = value or _identity

        def remember(item: object) -> None:
            records.append(PubSubMarbleRecord(self.frame, value_of(item)))

        subscribe = getattr(source, "subscribe", None)
        if not callable(subscribe):
            raise TypeError("source must provide subscribe")
        subscription = subscribe(remember)
        dispose = getattr(subscription, "dispose", None)
        if callable(dispose):
            self._subscriptions.append(subscription)
        return records

    @property
    def frame(self) -> int:
        """Return the current virtual marble frame."""
        return getattr(self, "_frame", 0)

    def publish(
        self,
        topic: PubSub,
        marbles: str,
        values: Mapping[str, object] | None = None,
    ) -> None:
        """Publish mapped marble values to ``topic`` on virtual frames."""
        self._ensure_open()
        value_map = values or {}
        for token in marbles:
            if token == " ":
                continue
            if token == "-":
                self._frame = self.frame + 1
                continue
            if token == "|":
                break
            if token == "#":
                raise ValueError("PubSub marble errors are not supported")
            topic.publish(value_map.get(token, token), event_time=self.frame)
            self._frame = self.frame + 1

    def run(
        self,
        topic: PubSub,
        marbles: str,
        values: Mapping[str, object] | None = None,
        *,
        observe: object | None = None,
        value: Callable[[object], object] | None = None,
    ) -> tuple[PubSubMarbleRecord, ...]:
        """Observe ``observe`` while publishing ``marbles`` to ``topic``."""
        records = self.observe(topic if observe is None else observe, value=value)
        self.publish(topic, marbles, values)
        return tuple(records)

    def close(self) -> None:
        """Dispose active subscriptions and release the owned runtime handle."""
        if self._closed:
            return
        self._closed = True
        for subscription in tuple(self._subscriptions):
            subscription.dispose()
        self._subscriptions.clear()
        self._fabric = None

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("PubSubMarbles is closed")


def _identity(value: object) -> object:
    return value


__all__ = [
    "PubSubMarbleRecord",
    "PubSubMarbles",
    "pubsub_marbles",
]
