from __future__ import annotations

import unittest
from dataclasses import dataclass

from manyfold.architecture import (
    HistoricalValue,
    ImmutableValue,
    NewValues,
    PubSub,
    PubSubCurrentValueSurface,
    PubSubTopic,
    PubSubValueSurface,
    Value,
    ValueSubscription,
)


@dataclass(frozen=True)
class Acceleration:
    x: float
    y: float
    z: float


class ArchitectureValueTests(unittest.TestCase):
    def test_immutable_value_replays_current_value_without_retaining_observer(
        self,
    ) -> None:
        value = ImmutableValue.initialized("locked", name="lock.owner")
        observed: list[str] = []

        subscription = value.observe(observed.append)

        self.assertEqual(value.name, "lock.owner")
        self.assertTrue(value.has_value)
        self.assertEqual(value.latest, "locked")
        self.assertEqual(observed, ["locked"])
        self.assertEqual(value.subscriber_count, 0)
        self.assertFalse(subscription.dispose())
        self.assertFalse(hasattr(value, "set"))
        self.assertFalse(hasattr(value, "from_stream"))

    def test_new_values_only_delivers_future_publications(self) -> None:
        values = NewValues[int](name="events")
        observed: list[int] = []

        values.publish(1)
        subscription = values.observe(observed.append)
        values.publish(2)
        values.publish(3)

        self.assertEqual(observed, [2, 3])
        self.assertEqual(values.subscriber_count, 1)
        self.assertTrue(subscription.dispose())
        self.assertTrue(subscription.is_disposed)
        self.assertFalse(subscription.dispose())
        values.publish(4)
        self.assertEqual(observed, [2, 3])
        self.assertEqual(values.subscriber_count, 0)
        self.assertEqual(values.name, "events")

    def test_new_values_accepts_observer_object_and_noop_subscription(self) -> None:
        values = NewValues[str]()
        observed: list[str] = []

        class Observer:
            def on_next(self, value: str) -> None:
                observed.append(value)

        noop = values.subscribe()
        observer_subscription = values.subscribe(Observer())
        values.emit("ready")

        self.assertIsInstance(noop, ValueSubscription)
        self.assertEqual(observed, ["ready"])
        self.assertEqual(values.subscriber_count, 2)
        noop.dispose()
        observer_subscription.dispose()
        self.assertEqual(values.subscriber_count, 0)

    def test_new_values_live_operators_delegate_to_pubsub_observable(self) -> None:
        values = NewValues[int]()
        observed: list[int] = []

        subscription = (
            values.scan(lambda total, value: total + value, seed=10)
            .map(lambda total: total * 2)
            .subscribe(observed.append)
        )
        values.publish(1)
        values.publish(2)

        self.assertEqual(observed, [22, 26])
        self.assertTrue(subscription.dispose())

    def test_value_replays_latest_to_late_subscribers(self) -> None:
        value = Value.initialized("first")
        observed: list[str] = []

        subscription = value.observe(observed.append)
        value.set("second")

        self.assertTrue(value.has_value)
        self.assertEqual(value.latest, "second")
        self.assertEqual(observed, ["first", "second"])
        subscription.dispose()

    def test_value_map_transforms_replayed_and_future_values(self) -> None:
        value = Value.initialized(2)
        observed: list[int] = []

        subscription = value.map(lambda item: item * 10).subscribe(observed.append)
        value.set(3)

        self.assertEqual(observed, [20, 30])
        self.assertTrue(subscription.dispose())

    def test_value_subscribe_accepts_callback_arguments(self) -> None:
        value = Value.initialized(2)
        observed: list[int] = []

        value.subscribe(observed.append, None, None)

        self.assertEqual(observed, [2])

    def test_value_can_skip_initial_replay(self) -> None:
        value = Value.initialized(10)
        observed: list[int] = []

        value.observe(observed.append, replay_latest=False)
        value.set(11)

        self.assertEqual(observed, [11])

    def test_uninitialized_value_has_no_latest_until_set(self) -> None:
        value = Value[int]()
        observed: list[int] = []

        value.subscribe(on_next=observed.append)
        self.assertFalse(value.has_value)
        self.assertIsNone(value.latest)

        value.set(7)

        self.assertTrue(value.has_value)
        self.assertEqual(value.latest, 7)
        self.assertEqual(observed, [7])
        self.assertFalse(hasattr(value, "from_stream"))

    def test_historical_value_retains_bounded_history_and_latest(self) -> None:
        history = HistoricalValue[int](retained_values=2)

        history.append(1)
        history.append(2)
        history.append(3)

        self.assertEqual(history.latest, 3)
        self.assertEqual(history.replay(), (2, 3))
        self.assertEqual(history.replay(limit=1), (3,))
        self.assertEqual(history.replay(limit=0), ())

    def test_historical_value_replays_retained_history_before_future_values(
        self,
    ) -> None:
        history = HistoricalValue[str](retained_values=3)
        observed: list[str] = []

        history.publish("a")
        history.publish("b")
        subscription = history.observe(observed.append, replay=True)
        history.publish("c")

        self.assertEqual(observed, ["a", "b", "c"])
        self.assertEqual(history.subscriber_count, 1)
        subscription.dispose()
        self.assertEqual(history.subscriber_count, 0)

    def test_historical_value_replay_can_be_limited_for_subscription(self) -> None:
        history = HistoricalValue[int](retained_values=4)
        observed: list[int] = []

        for item in (1, 2, 3):
            history.append(item)
        history.subscribe(observed.append, replay=2)

        self.assertEqual(observed, [2, 3])

    def test_historical_value_rejects_invalid_bounds(self) -> None:
        with self.assertRaisesRegex(ValueError, "retained_values"):
            HistoricalValue(retained_values=0)

        history = HistoricalValue[int]()
        with self.assertRaisesRegex(ValueError, "limit"):
            history.replay(limit=-1)
        with self.assertRaisesRegex(ValueError, "replay"):
            history.observe(lambda _value: None, replay=-1)

    def test_observer_must_be_callable_or_expose_on_next(self) -> None:
        values = NewValues[int]()

        with self.assertRaisesRegex(TypeError, "observer"):
            values.observe(object())

    def test_named_new_values_can_be_backed_by_pubsub_stream(self) -> None:
        values = NewValues[Acceleration](name="current_acceleration_value").from_stream(
            namespace="value-tests",
            schema=Acceleration,
        )
        observed: list[Acceleration] = []

        values.observe(observed.append)
        values.set(Acceleration(x=1.0, y=2.0, z=3.0))

        same_handle = PubSubTopic(
            "current_acceleration_value",
            namespace="value-tests",
            schema=Acceleration,
        )
        latest = same_handle.latest()

        self.assertEqual(observed, [Acceleration(x=1.0, y=2.0, z=3.0)])
        self.assertIsNotNone(latest)
        self.assertEqual(
            latest.as_model(Acceleration), Acceleration(x=1.0, y=2.0, z=3.0)
        )

    def test_historical_value_from_stream_replays_bounded_pubsub_history(self) -> None:
        history = HistoricalValue[Acceleration](
            name="acceleration_history",
            retained_values=2,
        ).from_stream(namespace="value-tests", schema=Acceleration)

        history.append(Acceleration(x=1.0, y=0.0, z=0.0))
        history.append(Acceleration(x=2.0, y=0.0, z=0.0))
        history.append(Acceleration(x=3.0, y=0.0, z=0.0))

        self.assertEqual(
            history.replay(),
            (
                Acceleration(x=2.0, y=0.0, z=0.0),
                Acceleration(x=3.0, y=0.0, z=0.0),
            ),
        )
        self.assertEqual(history.latest, Acceleration(x=3.0, y=0.0, z=0.0))

    def test_pubsub_value_latest_returns_immutable_value_handle(self) -> None:
        pubsub = PubSub(topic="value.surface.acceleration", schema=Acceleration)

        pubsub.publish(Acceleration(x=7.0, y=8.0, z=9.0))
        current = pubsub.value.latest()

        self.assertIsInstance(pubsub.value, PubSubValueSurface)
        self.assertIsInstance(current, ImmutableValue)
        self.assertEqual(current.name, "value.surface.acceleration.latest")
        self.assertTrue(current.has_value)
        self.assertEqual(
            current.latest.as_model(Acceleration), Acceleration(x=7.0, y=8.0, z=9.0)
        )
        self.assertFalse(hasattr(current, "set"))

    def test_pubsub_value_current_latest_returns_mutable_value_handle(self) -> None:
        pubsub = PubSub(topic="value.surface.current", schema=Acceleration)

        pubsub.publish(Acceleration(x=10.0, y=11.0, z=12.0))
        current = pubsub.value.current.latest()

        self.assertIsInstance(pubsub.value.current, PubSubCurrentValueSurface)
        self.assertIsInstance(current, Value)
        self.assertEqual(current.name, "value.surface.current.current")
        self.assertTrue(current.has_value)
        self.assertEqual(
            current.latest.as_model(Acceleration),
            Acceleration(x=10.0, y=11.0, z=12.0),
        )

        current.set({"x": 13.0, "y": 14.0, "z": 15.0})

        self.assertEqual(current.latest, {"x": 13.0, "y": 14.0, "z": 15.0})


if __name__ == "__main__":
    unittest.main()
