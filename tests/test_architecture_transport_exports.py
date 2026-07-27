from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from manyfold import architecture
from manyfold.architecture.enrollment import NodeIdentityStore
from manyfold.architecture.machine_signer import (
    MachineSignerClient,
    MachineSignerService,
)
from manyfold.architecture.transport import NodeIdentity
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

    @unittest.skipUnless(os.name == "nt", "Windows import boundary")
    def test_windows_imports_architecture_before_rejecting_posix_identity_ops(
        self,
    ) -> None:
        self.assertIs(architecture.DurableDelivery, DurableDelivery)
        self.assertIs(architecture.NodeIdentityStore, NodeIdentityStore)
        self.assertIs(architecture.MachineSignerService, MachineSignerService)
        self.assertIs(architecture.MachineSignerClient, MachineSignerClient)

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            missing = root / "missing"
            with self.assertRaisesRegex(
                NotImplementedError,
                "requires POSIX advisory file locking",
            ):
                NodeIdentityStore.open(missing)
            self.assertFalse(missing.exists())

            identity_root = root / "identity"
            with self.assertRaisesRegex(
                NotImplementedError,
                "requires POSIX advisory file locking",
            ):
                NodeIdentityStore.initialize(identity_root, node_id="windows")
            self.assertFalse(identity_root.exists())

            process_directories_before = set(
                Path(tempfile.gettempdir()).glob(
                    "manyfold-process-identity-*"
                )
            )
            with self.assertRaisesRegex(
                NotImplementedError,
                "requires POSIX Unix sockets and advisory file locking",
            ):
                MachineSignerClient(
                    root / "signer.sock",
                    NodeIdentity("cluster", "windows"),
                )
            self.assertEqual(
                set(
                    Path(tempfile.gettempdir()).glob(
                        "manyfold-process-identity-*"
                    )
                ),
                process_directories_before,
            )


if __name__ == "__main__":
    unittest.main()
