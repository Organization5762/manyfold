from __future__ import annotations

from typing import TypedDict

from manyfold import Graph
from manyfold import MailboxDescriptor
from manyfold import OwnerName
from manyfold import Plane
from manyfold import Schema
from manyfold import StreamFamily
from manyfold import StreamName
from manyfold import Variant
from manyfold.graph import route
from manyfold._manyfold_rust import Layer


class MailboxBridgeExampleResult(TypedDict):
    capacity: int
    overflow_policy: str
    topology_edges: tuple[tuple[str, str], ...]


def run_example() -> MailboxBridgeExampleResult:
    graph = Graph()
    mailbox_descriptor = MailboxDescriptor(capacity=4, overflow_policy="drop_oldest")
    mailbox = graph.mailbox("bridge", mailbox_descriptor)
    producer_route = route(
        plane=Plane.Read,
        layer=Layer.Logical,
        owner=OwnerName("sensor"),
        family=StreamFamily("mailbox"),
        stream=StreamName("producer"),
        variant=Variant.Meta,
        schema=Schema.bytes("MailboxProducer"),
    )

    consumer_route = route(
        plane=Plane.Write,
        layer=Layer.Logical,
        owner=OwnerName("consumer"),
        family=StreamFamily("mailbox"),
        stream=StreamName("consumer"),
        variant=Variant.Request,
        schema=Schema.bytes("MailboxConsumer"),
    )

    graph.connect(producer_route, mailbox)
    graph.connect(mailbox, consumer_route)

    return {
        "capacity": mailbox_descriptor.capacity,
        "overflow_policy": "drop_oldest",
        "topology_edges": tuple(graph.topology()),
    }


if __name__ == "__main__":
    print(run_example())

