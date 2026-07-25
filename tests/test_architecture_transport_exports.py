from __future__ import annotations

import unittest

from manyfold import architecture
from manyfold.architecture.transport_delivery import DurableDelivery
from manyfold.architecture.transport_mesh import TransportMesh
from manyfold.architecture.transport_pki import TlsSecurityReloader
from manyfold.architecture.transport_rpc import RpcEndpoint


class ArchitectureTransportExportTests(unittest.TestCase):
    def test_integrated_transport_layers_are_public_architecture_types(self) -> None:
        self.assertIs(architecture.DurableDelivery, DurableDelivery)
        self.assertIs(architecture.TransportMesh, TransportMesh)
        self.assertIs(architecture.TlsSecurityReloader, TlsSecurityReloader)
        self.assertIs(architecture.RpcEndpoint, RpcEndpoint)

    def test_integrated_transport_contract_is_declared_in_module_exports(self) -> None:
        expected = {
            "DeliveryConfig",
            "DurableDelivery",
            "MeshConfig",
            "MutualTlsFiles",
            "RpcConfig",
            "RpcEndpoint",
            "TlsSecurityReloader",
            "TransportMesh",
        }

        self.assertLessEqual(expected, set(architecture.__all__))


if __name__ == "__main__":
    unittest.main()
