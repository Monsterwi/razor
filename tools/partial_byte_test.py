"""Client completion and independent current-chunk cache completion over real HTTP."""
import concurrent.futures
from contextlib import ExitStack
import hashlib
import http.client
import io
import json
import os
from pathlib import Path
import socket
import struct
import tempfile
import threading
from types import SimpleNamespace
import unittest

from acceptance import Client, MIB, Origin, Product, create_fixtures, wait_for
from public_acceptance import drain, snapshot


@unittest.skipUnless(os.environ.get("RAZOR_ACCEPTANCE_BINARY"), "set RAZOR_ACCEPTANCE_BINARY for real single-byte HTTP")
class SingleByteRangeTest(unittest.TestCase):
    def setUp(self):
        self.cleanup = ExitStack()
        self.addCleanup(self.cleanup.close)
        root = Path(self.cleanup.enter_context(tempfile.TemporaryDirectory()))
        self.root = root
        self.total = 3 * MIB + 37
        fixtures = create_fixtures(root / "fixtures", self.total)
        self.origin = Origin(fixtures, root / "origin.json")
        self.cleanup.callback(self.origin.close)
        self.client = Client(fixtures, root)
        self.cleanup.callback(self.client.raw.close)
        args = SimpleNamespace(binary=Path(os.environ["RAZOR_ACCEPTANCE_BINARY"]), cache_root=None,
            public_static=True, ram_algorithm="otter", ram_bytes=10 * MIB, block_bytes=MIB)
        self.product = Product("razor", root / "single-byte", self.origin, args, capacity=32)
        self.cleanup.callback(self.product.stop)

    def held_origin(self, path, position=16 << 10, truncate_at=None):
        entered, gate = threading.Event(), threading.Event()
        self.cleanup.callback(gate.set)
        self.origin.rules[path] = {"body_gate_at": position, "entered": entered, "gate": gate}
        if truncate_at is not None:
            self.origin.rules[path]["truncate_at"] = truncate_at
        return entered, gate

    def raw_range(self, path, start, end):
        connection = socket.create_connection(("127.0.0.1", self.product.port), timeout=5)
        self.cleanup.callback(connection.close)
        connection.sendall((f"GET {path} HTTP/1.1\r\nHost: bench.local\r\nAccept-Encoding: identity\r\n"
                            f"Range: bytes={start}-{end}\r\nConnection: close\r\n\r\n").encode())
        # Read only headers: buffered clients must not hide the body's timing.
        raw = bytearray()
        while not raw.endswith(b"\r\n\r\n"):
            byte = connection.recv(1)
            self.assertTrue(byte, "connection closed before response headers")
            raw.extend(byte)
            self.assertLess(len(raw), 64 << 10)
        status, rest = bytes(raw).split(b"\r\n", 1)
        self.assertEqual(status.split()[1], b"206")
        headers = http.client.parse_headers(io.BytesIO(rest))
        self.assertEqual(headers["Content-Length"], str(end - start + 1))
        self.assertEqual(headers["Content-Range"], f"bytes {start}-{end}/{self.total}")
        self.assertEqual(headers["ETag"], '"large-v1"')
        return connection

    def assert_body(self, path, body, bounds):
        self.assertEqual((len(body), hashlib.sha256(body).hexdigest()), self.client.expected(path, bounds=bounds))

    def checked_range(self, path, bounds):
        row = self.client.checked(self.product, path, headers={"Range": f"bytes={bounds[0]}-{bounds[1]}"}, status=206,
                                  expected=self.client.expected(path, bounds=bounds))
        headers = {name.lower(): value for name, value in row["headers"].items()}
        self.assertEqual(headers["content-length"], str(bounds[1] - bounds[0] + 1))
        self.assertEqual(headers["content-range"], f"bytes {bounds[0]}-{bounds[1]}/{self.total}")
        self.assertEqual(headers["etag"], '"large-v1"')

    def finished_origin(self, path):
        drain(self.product)
        wait_for(lambda: all("end" in row for row in self.origin.records if row["path"] == path), "origin finished")
        return self.origin.snapshot(path)

    def origin_ranges(self, path):
        return [row["range"] for row in self.origin.records if row["path"] == path and row["method"] == "GET"]

    def test_single_byte_finishes_early_and_current_chunk_continues_without_clients(self):
        path = "/large-one-byte"
        entered, gate = self.held_origin(path)
        single = self.raw_range(path, 7, 7)
        self.assertTrue(entered.wait(5))
        self.assert_body(path, single.recv(2), (7, 7))
        self.assertEqual(single.recv(1), b"", "client completion still waits for chunk validation")
        single.close()
        self.assertFalse(gate.is_set())
        self.assertEqual(self.origin.snapshot(path), {"requests": 2, "get": 1, "head": 1, "bytes": 16 << 10})
        # A later reader joins the independently continuing current chunk.
        self.checked_range(path, (0, 8191))
        self.assertFalse(gate.is_set())
        self.assertEqual(self.origin.snapshot(path)["get"], 1)
        gate.set()
        before = self.finished_origin(path)
        self.assertEqual(before, {"requests": 2, "get": 1, "head": 1, "bytes": MIB})
        self.assertEqual(self.origin_ranges(path), [f"bytes=0-{MIB - 1}"])
        self.checked_range(path, (0, MIB - 1))
        self.assertEqual(self.origin.snapshot(path), before, "completed chunk was not retained after clients left")

    def test_batch_counts_independent_fill_after_client_completion(self):
        path = "/large-measured-early-byte"
        entered, gate = self.held_origin(path)
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(self.client.batch, self.product, self.origin, "early-byte", path, 1,
                                     count=1, bounds=(7, 7), headers={"Range": "bytes=7-7"})
                try:
                    self.assertTrue(entered.wait(5))
                    def client_finished():
                        rows = [json.loads(line) for line in (self.root / "requests.jsonl").read_text().splitlines(keepends=True) if line.endswith("\n")]
                        return any(row["case"] == "early-byte" and row["bytes"] == 1 and not row["error"] for row in rows)
                    wait_for(client_finished, "one-byte client completed before origin tail")
                    self.assertFalse(gate.is_set())
                    self.assertEqual(self.origin.snapshot(path)["bytes"], 16 << 10)
                    # The response is complete; accounting must still wait for its fill.
                    with self.assertRaises(concurrent.futures.TimeoutError):
                        future.result(timeout=.1)
                finally:
                    gate.set()
                measured = future.result(timeout=5)
        finally:
            gate.set()
        self.assertEqual(measured["response_body_bytes"], 1)
        self.assertEqual(measured["origin"], {"requests": 2, "get": 1, "head": 1, "bytes": MIB})
        self.assertEqual(measured["origin_body_bytes_per_response_byte"], MIB)
        self.assertGreater(measured["fill_settle_seconds"], 0)
        self.assertGreater(measured["observation_seconds"], measured["seconds"])
        hot = self.client.batch(self.product, self.origin, "early-byte-hot", path, 1,
                               count=1, bounds=(7, 7), headers={"Range": "bytes=7-7"})
        self.assertEqual(hot["origin"], {"requests": 0, "get": 0, "head": 0, "bytes": 0})

    def test_disconnected_last_reader_still_completes_current_chunk(self):
        path = "/large-disconnected-byte-reader"
        entered, gate = self.held_origin(path)
        connection = self.raw_range(path, 0, MIB - 1)
        self.assertTrue(entered.wait(5))
        prefix = bytearray()
        while len(prefix) < 8192:
            part = connection.recv(8192 - len(prefix))
            self.assertTrue(part, "response ended before its available prefix")
            prefix.extend(part)
        self.assert_body(path, bytes(prefix), (0, 8191))
        connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        connection.close()
        wait_for(lambda: snapshot(self.product)["stats"]["InFlight"] == 0, "disconnected HTTP handler released")
        self.assertFalse(gate.is_set())
        gate.set()
        before = self.finished_origin(path)
        self.assertEqual(before, {"requests": 2, "get": 1, "head": 1, "bytes": MIB})
        self.assertEqual(self.origin_ranges(path), [f"bytes=0-{MIB - 1}"])
        self.checked_range(path, (0, MIB - 1))
        self.assertEqual(self.origin.snapshot(path), before)

    def test_later_truncation_keeps_early_success_but_not_failed_chunk(self):
        path, gate_at, truncated_at = "/large-early-byte-truncated", MIB + (16 << 10), MIB + (32 << 10)
        entered, gate = self.held_origin(path, gate_at, truncated_at)
        follower = http.client.HTTPConnection("127.0.0.1", self.product.port, timeout=5)
        self.cleanup.callback(follower.close)
        follower.request("GET", path, headers={"Host": "bench.local", "Accept-Encoding": "identity", "Range": f"bytes=0-{2 * MIB - 1}"})
        response = follower.getresponse()
        self.assertEqual(response.status, 206)
        self.assertEqual(response.getheader("Content-Length"), str(2 * MIB))
        self.assertEqual(response.getheader("Content-Range"), f"bytes 0-{2 * MIB - 1}/{self.total}")
        prefix = response.read(gate_at)
        self.assert_body(path, prefix, (0, gate_at - 1))
        self.assertTrue(entered.wait(5))
        single = self.raw_range(path, MIB + 7, MIB + 7)
        self.assert_body(path, single.recv(2), (MIB + 7, MIB + 7))
        self.assertEqual(single.recv(1), b"")
        single.close()
        self.assertFalse(gate.is_set(), "early successful response required the origin tail")
        self.assertEqual(self.origin.snapshot(path), {"requests": 2, "get": 1, "head": 1, "bytes": gate_at})
        gate.set()
        with self.assertRaises(http.client.IncompleteRead) as incomplete:
            response.read()
        self.assert_body(path, prefix + incomplete.exception.partial, (0, truncated_at - 1))
        failed = self.finished_origin(path)
        self.assertEqual(failed, {"requests": 2, "get": 1, "head": 1, "bytes": truncated_at})
        self.checked_range(path, (0, MIB - 1))
        self.assertEqual(self.origin.snapshot(path), failed, "complete first chunk was discarded with failed tail")

        self.origin.rules[path] = {}
        self.checked_range(path, (MIB + 7, MIB + 7))
        rebuilt = self.finished_origin(path)
        self.assertEqual(rebuilt["get"], 2, "failed chunk was reused or retried within the original request")
        self.assertEqual(rebuilt["bytes"], truncated_at + MIB)
        self.assertEqual(self.origin_ranges(path), [f"bytes=0-{2 * MIB - 1}", f"bytes={MIB}-{2 * MIB - 1}"])
        self.checked_range(path, (0, 2 * MIB - 1))
        self.assertEqual(self.origin.snapshot(path), rebuilt)
