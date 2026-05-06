from __future__ import annotations

from typing import TypeVar

from manyfold import (
    Layer,
    OwnerName,
    Plane,
    Schema,
    StreamFamily,
    StreamName,
    TypedRoute,
    Variant,
    route,
)

__all__ = ("example_route", "int_schema", "sibling_route")

T = TypeVar("T")


def int_schema(schema_id: str, version: int = 1) -> "Schema[int]":
    """ASCII integer schema used by the executable examples."""
    return Schema(
        schema_id=schema_id,
        version=version,
        encode=lambda value: str(value).encode("ascii"),
        decode=lambda payload: int(payload.decode("ascii")),
    )


def example_route(
    *,
    plane: "Plane",
    layer: "Layer",
    owner: str,
    family: str,
    stream: str,
    variant: "Variant",
    schema: "Schema[T]",
) -> "TypedRoute[T]":
    """Build a typed route for the executable examples with string inputs."""
    return route(
        plane=plane,
        layer=layer,
        owner=OwnerName(owner),
        family=StreamFamily(family),
        stream=StreamName(stream),
        variant=variant,
        schema=schema,
    )


def sibling_route(
    base: "TypedRoute[T]",
    *,
    plane: "Plane | None" = None,
    layer: "Layer | None" = None,
    owner: str | None = None,
    family: str | None = None,
    stream: str | None = None,
    variant: "Variant | None" = None,
    schema: "Schema[T] | None" = None,
) -> "TypedRoute[T]":
    """Clone one example route while overriding only the changed fields."""
    return route(
        plane=base.plane if plane is None else plane,
        layer=base.layer if layer is None else layer,
        owner=base.owner if owner is None else OwnerName(owner),
        family=base.family if family is None else StreamFamily(family),
        stream=base.stream if stream is None else StreamName(stream),
        variant=base.variant if variant is None else variant,
        schema=base.schema if schema is None else schema,
    )
