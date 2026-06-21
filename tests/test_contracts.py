from __future__ import annotations

import unittest
from dataclasses import dataclass

from manyfold import Contract, Graph, Layer, Plane, Variant, route


@dataclass(frozen=True)
class TemperatureV1:
    celsius: float


@dataclass(frozen=True)
class TemperatureV2:
    celsius: float
    sampled_at_ms: int | None = None


@dataclass(frozen=True)
class Humidity:
    percent: float


class ContractTests(unittest.TestCase):
    def test_contract_maps_accepted_upstream_type_to_current_type(self) -> None:
        contract = Contract.of(TemperatureV2).accepts(
            TemperatureV1,
            lambda old: TemperatureV2(celsius=old.celsius),
        )

        self.assertEqual(
            contract.convert(TemperatureV1(celsius=21.5)),
            TemperatureV2(celsius=21.5),
        )
        self.assertEqual(
            contract.convert(TemperatureV2(celsius=22.0, sampled_at_ms=12)),
            TemperatureV2(celsius=22.0, sampled_at_ms=12),
        )
        with self.assertRaisesRegex(ValueError, "not compatible"):
            contract.convert(Humidity(percent=50.0))

    def test_contract_can_back_a_typed_route_without_schema_strings(self) -> None:
        temperature = route(
            plane=Plane.Read,
            layer=Layer.Logical,
            owner="thermostat",
            family="environment",
            stream="temperature",
            variant=Variant.Meta,
            schema=Contract.of(TemperatureV2),
        )
        graph = Graph()

        graph.publish(temperature, TemperatureV2(celsius=21.5, sampled_at_ms=100))
        latest = graph.latest(temperature)

        self.assertIsNotNone(latest)
        if latest is None:
            raise AssertionError("expected a latest temperature envelope")
        self.assertEqual(
            latest.value,
            TemperatureV2(celsius=21.5, sampled_at_ms=100),
        )
        self.assertEqual(
            temperature.schema.schema_id,
            f"{TemperatureV2.__module__}.{TemperatureV2.__qualname__}",
        )

    def test_dataclass_type_can_be_used_as_route_contract_descriptor(self) -> None:
        temperature = route(
            plane=Plane.Read,
            layer=Layer.Logical,
            owner="thermostat",
            family="environment",
            stream="temperature",
            variant=Variant.Meta,
            schema=TemperatureV2,
        )

        encoded = temperature.schema.encode(TemperatureV2(celsius=20.0))

        self.assertEqual(
            temperature.schema.decode(encoded),
            TemperatureV2(celsius=20.0),
        )


if __name__ == "__main__":
    unittest.main()
