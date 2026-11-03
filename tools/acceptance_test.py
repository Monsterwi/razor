import os
import json
import time
from contextlib import ExitStack
from pathlib import Path
import sys
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import acceptance


class StartupCleanupTest(unittest.TestCase):
    def product(self, directory, command):
        product = acceptance.Product.__new__(acceptance.Product)
        product.dir = Path(directory)
        product.command = command
        product.env = os.environ.copy()
        product.kind = "test-child"
        product.port = acceptance.free_port()
        product.proc = None
        def cleanup():
            if product.proc is not None:
                if product.proc.poll() is None:
                    product.proc.kill()
                product.proc.wait()
            if hasattr(product, "log"):
                product.log.close()
        self.addCleanup(cleanup)
        return product

    def test_readiness_timeout_reaps_real_child_and_closes_log(self):
        with tempfile.TemporaryDirectory() as directory:
            product = self.product(directory, [sys.executable, "-c", "import time; time.sleep(60)"])
            real_wait = acceptance.wait_for
            def short_wait(check, description, timeout):
                return real_wait(check, description, .1)
            with patch.object(acceptance, "wait_for", short_wait):
                with self.assertRaisesRegex(AssertionError, "timeout: test-child listening"):
                    product.start()
            self.assertIsNotNone(product.proc.returncode, "startup failure did not reap child")
            self.assertTrue(product.log.closed, "startup failure leaked process log")

    def test_popen_failure_closes_log_and_keeps_original_error(self):
        with tempfile.TemporaryDirectory() as directory:
            product = self.product(directory, [str(Path(directory) / "missing-binary")])
            with self.assertRaises(FileNotFoundError):
                product.start()
            self.assertTrue(product.log.closed, "Popen failure leaked process log")

    def test_constructor_failure_closes_real_origins_and_client(self):
        for run in (acceptance.functional, acceptance.benchmark):
            with self.subTest(run=run.__name__), tempfile.TemporaryDirectory() as directory:
                origins, clients = [], []
                real_origin, real_client = acceptance.Origin, acceptance.Client
                def origin(*args, **kwargs):
                    instance = real_origin(*args, **kwargs)
                    origins.append(instance)
                    self.addCleanup(lambda server=instance.server: (server.shutdown(), server.server_close()))
                    return instance
                def client(*args, **kwargs):
                    instance = real_client(*args, **kwargs)
                    clients.append(instance)
                    self.addCleanup(instance.raw.close)
                    return instance
                args = SimpleNamespace(binary=Path(directory) / "missing-binary", products=["razor"])
                with patch.object(acceptance, "Origin", origin), patch.object(acceptance, "Client", client):
                    with self.assertRaises(FileNotFoundError):
                        run(args, {}, Path(directory))
                self.assertTrue(all(not instance.thread.is_alive() for instance in origins))
                self.assertTrue(all(instance.server.fileno() == -1 for instance in origins))
                self.assertTrue(all(instance.raw.closed for instance in clients))


class PortReservationTest(unittest.TestCase):
    def test_listener_port_avoids_ephemeral_and_actual_other_address_occupancy(self):
        import socket
        low, high = map(int, Path("/proc/sys/net/ipv4/ip_local_port_range").read_text().split())
        port = acceptance.free_port()
        self.assertGreaterEqual(port, 1024)
        self.assertFalse(low <= port <= high, (port, low, high))
        with socket.socket() as occupied:
            occupied.bind(("127.0.0.2", port))
            occupied.listen()
            def occupied_first(candidates):
                candidates.remove(port)
                candidates.insert(0, port)
            with patch.object(acceptance.random, "shuffle", side_effect=occupied_first):
                selected = acceptance.free_port()
            self.assertNotEqual(selected, port)
            self.assertFalse(low <= selected <= high)


class PublicCLITest(unittest.TestCase):
    def test_tools_reject_nonfixed_chunk_size_before_side_effects(self):
        for script in ("acceptance.py", "hey_benchmark.py"):
            for block in (65536, 4194304):
                with self.subTest(script=script, block_bytes=block), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    command = [sys.executable, str(Path(__file__).with_name(script)),
                               "--binary", str(root / "missing-razor"),
                               "--output", str(root / "output"), "--cache-root", str(root / "cache"),
                               "--block-bytes", str(block)]
                    if script == "hey_benchmark.py":
                        command += ["--hey", str(root / "missing-hey"), "--fixtures", str(root / "fixtures")]
                    result = subprocess.run(command, capture_output=True, text=True, timeout=10)
                    self.assertEqual(result.returncode, 2, result.stderr)
                    self.assertIn("--block-bytes: invalid choice", result.stderr)
                    self.assertFalse((root / "output").exists())
                    self.assertFalse((root / "cache").exists())

    def test_public_ats_combinations_rejected_before_side_effects(self):
        for mode in ("benchmark", "all"):
            for products in (("ats",), ("razor", "ats")):
                with self.subTest(mode=mode, products=products), tempfile.TemporaryDirectory() as directory:
                    output, cache = Path(directory) / "output", Path(directory) / "cache"
                    argv = ["acceptance.py", "--binary", "/missing/razor", "--output", str(output),
                            "--cache-root", str(cache), "--mode", mode, "--public-static", "--products", *products,
                            "--ats-environment", "/missing/ats.json"]
                    with patch.object(sys, "argv", argv), patch.object(acceptance, "create_fixtures", side_effect=AssertionError("fixture creation reached")) as fixtures, patch.object(acceptance.Product, "start") as process:
                        with self.assertRaisesRegex(AssertionError, "public-static requires --products razor"):
                            acceptance.main()
                        fixtures.assert_not_called()
                        process.assert_not_called()
                    self.assertFalse(output.exists())
                    self.assertFalse(cache.exists())

    def test_public_razor_modes_and_conservative_ats_route_correctly(self):
        import public_acceptance
        for mode, public, products in (("benchmark", True, ["razor"]), ("all", True, ["razor"]), ("benchmark", False, ["ats"])):
            with self.subTest(mode=mode, public=public), tempfile.TemporaryDirectory() as directory, ExitStack() as cleanup:
                argv = ["acceptance.py", "--binary", "/missing/razor", "--output", str(Path(directory) / "output"),
                        "--mode", mode, "--products", *products, "--ats-environment", "/missing/ats.json",
                        "--block-bytes", "1048576"]
                if public:
                    argv.append("--public-static")
                cleanup.enter_context(patch.object(sys, "argv", argv))
                cleanup.enter_context(patch.object(acceptance, "create_fixtures", return_value={}))
                cleanup.enter_context(patch.object(acceptance, "file_hash", return_value="probe-hash"))
                cleanup.enter_context(patch.object(acceptance.subprocess, "check_output", return_value="probe"))
                conservative = cleanup.enter_context(patch.object(acceptance, "benchmark"))
                cleanup.enter_context(patch.object(acceptance, "functional"))
                public_functional = cleanup.enter_context(patch.object(public_acceptance, "public_functional"))
                public_benchmark = cleanup.enter_context(patch.object(public_acceptance, "public_benchmark"))
                acceptance.main()
                selected = public_benchmark if public else conservative
                selected.assert_called_once()
                self.assertEqual(selected.call_args.args[0].products, products)
                (conservative if public else public_benchmark).assert_not_called()
                self.assertEqual(public_functional.call_count, int(public and mode == "all"))


class DeviceSamplingTest(unittest.TestCase):
    def test_diskstats_uses_sectors_not_completed_operations(self):
        text = "8 32 sdc 100 2 1234 7 200 3 5678 9 0 456 789\n8 33 sdc1 1 0 1 0 2 0 2 0 0 3 4\n"
        self.assertEqual(acceptance.parse_device_io(os.makedev(8,32), text),
            {"major":8,"minor":32,"name":"sdc","read_bytes":631808,"write_bytes":2907136,"io_ms":456})
        self.assertIsNone(acceptance.parse_device_io(os.makedev(0,1), text))


class WorkloadTest(unittest.TestCase):
    def test_fixtures_keep_exact_size_tail_and_independent_versions(self):
        with tempfile.TemporaryDirectory() as directory:
            fixtures = acceptance.create_fixtures(Path(directory) / "fixtures", acceptance.CHUNK + 17)
            for version in (1, 2):
                self.assertEqual(fixtures[("small", version)].stat().st_size, 256 << 10)
                self.assertEqual(fixtures[("large", version)].stat().st_size, acceptance.CHUNK + 17)
            self.assertNotEqual(acceptance.file_hash(fixtures[("large", 1)]),
                                acceptance.file_hash(fixtures[("large", 2)]))

    def test_mixed_and_unique_requests_check_each_real_response(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as cleanup:
            root = Path(directory)
            fixtures = acceptance.create_fixtures(root / "fixtures", acceptance.CHUNK + 17)
            origin = acceptance.Origin(fixtures, root / "origin.json")
            cleanup.callback(origin.close)
            (root / "process.log").touch()
            client = acceptance.Client(fixtures, root)
            cleanup.callback(client.raw.close)
            product = SimpleNamespace(kind="test-origin", port=origin.server.server_port, dir=root,
                resource=lambda: {"time": 0, "cpu_seconds": 0, "rss_bytes": 0, "fds": 0,
                                  "read_bytes": 0, "write_bytes": 0})
            mixed = client.batch(product, origin, "mixed", "/small-mixed", 1, count=2,
                                 paths=["/small-mixed", "/large-mixed"])
            self.assertEqual(mixed["object_requests"], {"small": 1, "large": 1})
            self.assertEqual(mixed["unexpected_errors"], 0)
            self.assertEqual(mixed["response_body_bytes"], (256 << 10) + acceptance.CHUNK + 17)
            self.assertEqual(mixed["origin_body_bytes_per_response_byte"], 1)
            unique = client.batch(product, origin, "unique", "/small-unique", 2, count=2, unique_paths=True)
            self.assertEqual(unique["origin"]["get"], 4)
            self.assertEqual(len({row["path"] for row in origin.records if "unique" in row["path"]}), 4)
            self.assertEqual(unique["unexpected_errors"], 0)
            self.assertEqual(unique["response_body_bytes"], 1 << 20)


if __name__ == "__main__":
    unittest.main()


class BatchStatsLogTest(unittest.TestCase):
    def test_batch_preserves_record_boundaries_and_rejects_bad_json(self):
        from public_acceptance import DRAIN_FIELDS
        with tempfile.TemporaryDirectory() as directory, ExitStack() as cleanup:
            root = Path(directory)
            fixtures = acceptance.create_fixtures(root / "fixtures", acceptance.MIB)
            origin = acceptance.Origin(fixtures, root / "origin.json")
            cleanup.callback(origin.close)
            client = acceptance.Client(fixtures, root)
            cleanup.callback(client.raw.close)
            log = root / "process.log"
            log.touch()
            stats = dict.fromkeys((*DRAIN_FIELDS, "Requests", "Hits", "Misses", "Bypass", "Bytes",
                "RangeBlockHits", "RangeBlockMisses", "RangeHitBytes", "RangeMissBytes", "SharedFillJoined",
                "SharedFillCompleted", "SharedFillFailed", "Cancelled", "GCCount", "GCPauseTotalNanoseconds", "TotalAlloc"), 0)
            stats["Origin"] = {"InFlight": 0}
            def record(label):
                return "HTTP resource stats JSON: " + json.dumps({**stats, "SnapshotUnixNano": time.time_ns(), "Sample": label}) + "\n"
            pending, signals = [], 0
            def emit(_):
                nonlocal signals
                signals += 1
                with log.open("a") as stream:
                    stream.write("".join(pending))
                    pending.clear()
                    stream.write(record("complete") + "Goroutines:1\n")
                    if signals == 2:
                        line = record("boundary")
                        stream.write(line[:10])
                        pending.append(line[10:])
            product = SimpleNamespace(kind="razor", port=origin.server.server_port, dir=root,
                proc=SimpleNamespace(send_signal=emit), resource=lambda: {
                    "cpu_seconds": 0, "rss_bytes": 0, "fds": 0, "read_bytes": 0, "write_bytes": 0})
            sample_origin, origin_samples = origin.snapshot, 0
            def sample_and_write_tail():
                nonlocal origin_samples
                result = sample_origin()
                origin_samples += 1
                if origin_samples == 2:
                    line = record("tail")
                    with log.open("a") as stream:
                        stream.write(line[:-5])
                    pending.append(line[-5:])
                elif origin_samples == 4:
                    with log.open("a") as stream:
                        stream.write('HTTP resource stats JSON: {broken}\n')
                return result
            with patch.object(origin, "snapshot", side_effect=sample_and_write_tail):
                result = client.batch(product, origin, "partial-stats", "/small-log", 1, count=1)
                with self.assertRaises(json.JSONDecodeError):
                    client.batch(product, origin, "malformed-stats", "/small-log", 1, count=1)
            self.assertEqual(result["requests"], 1)
            self.assertEqual(result["unexpected_errors"], 0)
            labels = [sample["Sample"] for sample in result["server_stats_samples"]]
            self.assertIn("boundary", labels, "batch start discarded a partial marker")
            self.assertNotIn("tail", labels, "batch included an unfinished record")
