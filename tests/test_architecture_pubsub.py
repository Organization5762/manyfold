from __future__ import annotations

import unittest

from manyfold.architecture.pubsub import InMemoryPubSub, PubSubMessage


class ArchitecturePubSubTests(unittest.TestCase):
    def test_in_memory_pubsub_publishes_to_named_subscription(self) -> None:
        pubsub = InMemoryPubSub(retained_messages=8)
        subscription = pubsub.subscribe("orders.created", name="orders")

        delivery = pubsub.publish("orders.created", b"order-123")
        messages = pubsub.poll(subscription.name)

        self.assertEqual(subscription.topic, "orders.created")
        self.assertEqual(delivery.topic, "orders.created")
        self.assertEqual(delivery.offset, 0)
        self.assertEqual(delivery.delivered_to, ["orders"])
        self.assertEqual(delivery.subscriber_count, 1)
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].topic, "orders.created")
        self.assertEqual(messages[0].payload, b"order-123")
        self.assertEqual(messages[0].offset, 0)

    def test_pubsub_supports_wildcard_subscription_and_offsets(self) -> None:
        pubsub = InMemoryPubSub(retained_messages=8)
        pubsub.subscribe("*", name="all-events")

        pubsub.publish("orders.created", b"created")
        pubsub.publish("orders.paid", b"paid")

        messages = pubsub.poll("all-events")

        self.assertEqual(
            [message.topic for message in messages],
            ["orders.created", "orders.paid"],
        )
        self.assertEqual([message.offset for message in messages], [0, 1])
        self.assertEqual(pubsub.topic_offsets(), {"orders.created": 0, "orders.paid": 1})

    def test_pubsub_can_replay_retained_messages_from_beginning(self) -> None:
        pubsub = InMemoryPubSub(retained_messages=2)
        pubsub.publish("events", b"first")
        pubsub.publish("events", b"second")
        pubsub.publish("events", b"third")

        pubsub.subscribe("events", name="replay", replay_from_beginning=True)
        messages = pubsub.poll("replay")

        self.assertEqual([message.payload for message in messages], [b"second", b"third"])
        self.assertEqual([message.offset for message in messages], [1, 2])
        self.assertEqual(pubsub.message_count, 2)

    def test_pubsub_latest_reads_latest_matching_topic(self) -> None:
        pubsub = InMemoryPubSub(retained_messages=8)
        pubsub.publish("a", b"one")
        pubsub.publish("b", b"two")
        pubsub.publish("a", b"three")

        latest_any = pubsub.latest()
        latest_a = pubsub.latest("a")

        self.assertIsNotNone(latest_any)
        self.assertIsNotNone(latest_a)
        self.assertEqual(latest_any.payload, b"three")
        self.assertEqual(latest_a.payload, b"three")

    def test_pubsub_rejects_invalid_bounds_and_payloads(self) -> None:
        with self.assertRaisesRegex(ValueError, "retained_messages"):
            InMemoryPubSub(retained_messages=0)

        pubsub = InMemoryPubSub()
        pubsub.subscribe("events", name="events")
        with self.assertRaisesRegex(ValueError, "non-empty bytes"):
            pubsub.publish("events", b"")
        with self.assertRaisesRegex(ValueError, "unknown subscription"):
            pubsub.poll("missing")
        with self.assertRaisesRegex(ValueError, "max_messages"):
            pubsub.poll("events", max_messages=0)

    def test_message_can_be_constructed_for_application_boundaries(self) -> None:
        message = PubSubMessage("client.events", b"payload", offset=7)

        self.assertEqual(message.topic, "client.events")
        self.assertEqual(message.payload, b"payload")
        self.assertEqual(message.offset, 7)


if __name__ == "__main__":
    unittest.main()
