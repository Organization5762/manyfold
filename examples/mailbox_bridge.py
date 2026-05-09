from __future__ import annotations

from typing import TypedDict

from manyfold import Graph, Layer, MailboxDescriptor, Plane, Schema, Variant

from ._shared import example_route


def run_example() -> MailboxBridgeExampleResult:
    graph = Graph()
    mailbox_descriptor = MailboxDescriptor(capacity=4, overflow_policy="drop_oldest")
    mailbox = graph.mailbox("bridge", mailbox_descriptor)
    producer_route = example_route(
        plane=Plane.Read,
        layer=Layer.Logical,
        owner="sensor",
        family="mailbox",
        stream="producer",
        variant=Variant.Meta,
        schema=Schema.bytes(name="MailboxProducer"),
    )

    consumer_route = example_route(
        plane=Plane.Write,
        layer=Layer.Logical,
        owner="consumer",
        family="mailbox",
        stream="consumer",
        variant=Variant.Request,
        schema=Schema.bytes(name="MailboxConsumer"),
    )

    graph.connect(source=producer_route, sink=mailbox)
    graph.connect(source=mailbox, sink=consumer_route)
    graph.publish(producer_route, b"one")
    graph.publish(producer_route, b"two")
    snapshot = graph.mailbox_snapshot(mailbox)

    return {
        "capacity": snapshot.capacity,
        "available_credit": snapshot.available_credit,
        "depth": snapshot.depth,
        "dropped_messages": snapshot.dropped_messages,
        "overflow_policy": snapshot.overflow_policy,
        "topology_edges": tuple(graph.topology()),
    }


class MailboxBridgeExampleResult(TypedDict):
    capacity: int
    available_credit: int
    depth: int
    dropped_messages: int
    overflow_policy: str
    topology_edges: tuple[tuple[str, str], ...]


if __name__ == "__main__":
    print(run_example())
