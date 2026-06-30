from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

import examples.runtime_substrate as substrate
from tests.test_support import subprocess_test_env


class RuntimeSubstrateTests(unittest.TestCase):
    def test_runtime_substrate_shifts_and_moves_dormant_processes(self) -> None:
        result = substrate.run_example()

        self.assertEqual(result["controller"], "manyfold-controller")
        self.assertEqual(result["single_runtime"], "laptop")
        self.assertEqual(
            result["cluster_runtimes"],
            ("laptop", "worker-a", "worker-b"),
        )
        self.assertEqual(result["dormant_processes"], 25)
        self.assertEqual(result["controller_dormant_buffer"], 2)
        self.assertEqual(result["controller_max_processes"], 32)
        self.assertEqual(result["moved"]["name"], "kafka-broker-a")
        self.assertEqual(result["moved"]["runtime"], "worker-b")

    def test_single_computer_runtime_retains_existing_shift(self) -> None:
        runtime = substrate.SingleComputerRuntime("node-a", root="/tmp/manyfold-node-a")
        spec = substrate.ShiftSpec(
            name="service-discovery",
            capability="service.discovery",
            data_dir=Path("service-discovery"),
        )

        shifted = runtime.shift(spec)
        retained = runtime.shift(spec)

        self.assertEqual(shifted.action, "shift")
        self.assertEqual(retained.action, "retain")
        self.assertEqual(shifted.handle, retained.handle)
        self.assertEqual(len(runtime.dormant_processes()), 9)

    def test_controller_maintains_dormant_buffer_by_spawning_slots(self) -> None:
        runtime = substrate.SingleComputerRuntime(
            "node-a",
            root="/tmp/manyfold-node-a",
            dormant_processes=1,
        )
        controller = substrate.ManyFoldController(
            "controller",
            primary=runtime,
            policy=substrate.ManyFoldControllerPolicy(
                dormant_buffer=2,
                max_processes=5,
                spawn_batch_size=1,
            ),
        )

        initial = controller.snapshot()
        shifted = controller.shift(
            substrate.ShiftSpec(
                name="service-discovery",
                capability="service.discovery",
                data_dir=Path("service-discovery"),
            )
        )
        current = controller.snapshot()

        self.assertEqual(initial.dormant_processes, 2)
        self.assertEqual(shifted.action, "shift")
        self.assertEqual(current.active_processes, 1)
        self.assertEqual(current.dormant_processes, 2)
        self.assertEqual(len(runtime.dormant_processes()), 2)

    def test_controller_rejects_shift_when_capacity_bound_is_exhausted(self) -> None:
        runtime = substrate.SingleComputerRuntime(
            "node-a",
            root="/tmp/manyfold-node-a",
            dormant_processes=1,
        )
        controller = substrate.ManyFoldController(
            "controller",
            primary=runtime,
            policy=substrate.ManyFoldControllerPolicy(
                dormant_buffer=1,
                max_processes=2,
                spawn_batch_size=1,
            ),
        )

        controller.shift(
            substrate.ShiftSpec(
                name="service-discovery",
                capability="service.discovery",
                data_dir=Path("service-discovery"),
            )
        )

        with self.assertRaisesRegex(RuntimeError, "cannot satisfy dormant process buffer"):
            controller.shift(
                substrate.ShiftSpec(
                    name="load-balancer",
                    capability="ingress.load_balance",
                    data_dir=Path("load-balancer"),
                )
            )

    def test_durable_pubsub_composes_replicated_log_and_consumer_group(self) -> None:
        cluster = substrate.RuntimeCluster(
            (
                substrate.SingleComputerRuntime("node-a", root="/tmp/node-a"),
                substrate.SingleComputerRuntime("node-b", root="/tmp/node-b"),
                substrate.SingleComputerRuntime("node-c", root="/tmp/node-c"),
            )
        )
        durable_log = substrate.ReplicatedDurableLog(
            queue=substrate.ManyFoldKafkaQueueClient(cluster)
        )
        durable = substrate.DurablePubSub(log=durable_log)
        try:
            delivery = substrate._publish_pubsub_application_message(
                durable,
                topic="orders.created",
                payload=b"order-123",
            )
        finally:
            durable.close()

        self.assertTrue(durable_log.profile.durable_log)
        self.assertTrue(durable_log.profile.replicated)
        self.assertEqual(durable_log.profile.required_acks, 2)
        self.assertEqual(durable_log.profile.replica_count, 3)
        self.assertEqual(delivery.backing, "durable_pubsub")
        self.assertEqual(delivery.delivered_to, ("default-consumer-group",))
        self.assertTrue(delivery.replayable)
        self.assertEqual(delivery.produced_offset, 0)
        self.assertEqual(delivery.consumed_offset, 0)

    def test_pubsub_upgrade_example_reports_relaxed_and_durable_backends(self) -> None:
        result = substrate.run_pubsub_upgrade_example()

        self.assertEqual(result["pubsub_backing"], "pubsub")
        self.assertEqual(result["pubsub_delivered_to"], ("topic-consumer",))
        self.assertFalse(result["pubsub_replayable"])
        self.assertEqual(result["durable_backing"], "durable_pubsub")
        self.assertEqual(result["durable_components"], substrate.DurablePubSub.components)
        self.assertEqual(result["durable_required_acks"], 2)
        self.assertEqual(result["durable_replica_count"], 3)
        self.assertEqual(result["durable_shifted_processes"], 8)

    def test_projection_into_can_target_local_client_stream(self) -> None:
        local_stream = substrate.LocalClientStreamSink(name="local-client")
        projection = substrate.StreamingProjection(
            name="order_summary",
            output_topic="client.order_summary",
            transform=lambda message: b"summary:" + message.payload,
        )

        delivery = projection.into(local_stream).consume(
            substrate.PubSubMessage(topic="orders.created", payload=b"order-123")
        )

        self.assertEqual(delivery.projection, "order_summary")
        self.assertEqual(delivery.source_topic, "orders.created")
        self.assertEqual(delivery.output_topic, "client.order_summary")
        self.assertEqual(delivery.delivered_to, "local-client")
        self.assertEqual(delivery.payload, b"summary:order-123")
        self.assertEqual(len(local_stream.messages), 1)
        self.assertEqual(local_stream.messages[0].topic, "client.order_summary")

    def test_runtime_substrate_benchmark_runs_as_json_cli(self) -> None:
        result = subprocess.run(
            (
                sys.executable,
                "-m",
                "examples.runtime_substrate",
                "--kafka-benchmark",
                "--requests",
                "64",
                "--mode",
                "full",
            ),
            check=False,
            capture_output=True,
            text=True,
            env=subprocess_test_env(),
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        output = json.loads(result.stdout)
        self.assertEqual(output["mode"], "full")
        self.assertEqual(output["requests"], 64)
        self.assertEqual(output["shifted_processes"], 8)
        self.assertEqual(output["broker_count"], 3)
        self.assertGreater(output["average_request_us"], 0.0)


if __name__ == "__main__":
    unittest.main()
