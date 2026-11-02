"""One-byte HTTP responses retain the aligned chunk's completion protection."""
from contextlib import ExitStack
import hashlib
import http.client
import io
import os
from pathlib import Path
import select
import socket
import tempfile
import threading
from types import SimpleNamespace
import unittest

from acceptance import Client, MIB, Origin, Product, create_fixtures, wait_for
from public_acceptance import drain


@unittest.skipUnless(os.environ.get("RAZOR_ACCEPTANCE_BINARY"), "set RAZOR_ACCEPTANCE_BINARY for real single-byte HTTP")
class SingleByteRangeTest(unittest.TestCase):
    def setUp(self):
        self.cleanup = ExitStack()
        self.addCleanup(self.cleanup.close)
        root = Path(self.cleanup.enter_context(tempfile.TemporaryDirectory()))
        fixtures = create_fixtures(root / "fixtures", MIB)
        self.origin = Origin(fixtures, root / "origin.json")
        self.cleanup.callback(self.origin.close)
        self.client = Client(fixtures, root)
        self.cleanup.callback(self.client.raw.close)
        args = SimpleNamespace(binary=Path(os.environ["RAZOR_ACCEPTANCE_BINARY"]), cache_root=None,
            public_static=True, ram_algorithm="otter", ram_bytes=10 * MIB, block_bytes=MIB)
        self.product = Product("razor", root / "single-byte", self.origin, args, capacity=32)
        self.cleanup.callback(self.product.stop)

    def held_responses(self, path, truncated=False):
        entered, gate = threading.Event(), threading.Event()
        self.cleanup.callback(gate.set)
        self.origin.rules[path] = {"body_gate_at": 16 << 10, "entered": entered, "gate": gate}
        if truncated:
            self.origin.rules[path]["truncate_at"] = 32 << 10
        single = socket.create_connection(("127.0.0.1", self.product.port), timeout=5)
        self.cleanup.callback(single.close)
        single.sendall((f"GET {path} HTTP/1.1\r\nHost: bench.local\r\nAccept-Encoding: identity\r\n"
                        "Range: bytes=7-7\r\nConnection: close\r\n\r\n").encode())
        # Consume only headers so a buffered HTTP client cannot hide an early body byte.
        raw = bytearray()
        while not raw.endswith(b"\r\n\r\n"):
            byte = single.recv(1)
            self.assertTrue(byte, "connection closed before response headers")
            raw.extend(byte)
            self.assertLess(len(raw), 64 << 10)
        status, rest = bytes(raw).split(b"\r\n", 1)
        self.assertEqual(status.split()[1], b"206")
        headers = http.client.parse_headers(io.BytesIO(rest))
        self.assertEqual(headers["Content-Length"], "1")
        self.assertEqual(headers["Content-Range"], f"bytes 7-7/{MIB}")
        self.assertEqual(headers["ETag"], '"large-v1"')
        self.assertTrue(entered.wait(5))
        follower = http.client.HTTPConnection("127.0.0.1", self.product.port, timeout=5)
        self.cleanup.callback(follower.close)
        follower.request("GET", path, headers={"Host": "bench.local", "Accept-Encoding": "identity", "Range": "bytes=0-8191"})
        response = follower.getresponse()
        self.assertEqual(response.status, 206)
        self.assertEqual(response.getheader("Content-Length"), "8192")
        self.assertEqual(response.getheader("Content-Range"), f"bytes 0-8191/{MIB}")
        prefix = response.read(8191)
        self.assertEqual((len(prefix), hashlib.sha256(prefix).hexdigest()), self.client.expected(path, bounds=(0, 8190)))
        self.assertFalse(gate.is_set())
        self.assertEqual(self.origin.snapshot(path), {"requests": 2, "get": 1, "head": 1, "bytes": 16 << 10})
        self.assertFalse(select.select([single], [], [], 0)[0], "single byte completed before aligned chunk validation")
        return single, response, gate

    def checked_byte(self, path):
        row = self.client.checked(self.product, path, headers={"Range": "bytes=7-7"}, status=206,
                                  expected=self.client.expected(path, bounds=(7, 7)))
        headers = {name.lower(): value for name, value in row["headers"].items()}
        self.assertEqual(headers["content-length"], "1")
        self.assertEqual(headers["content-range"], f"bytes 7-7/{MIB}")
        self.assertEqual(headers["etag"], '"large-v1"')

    def test_cold_single_byte_waits_for_valid_chunk_then_becomes_hot(self):
        path = "/large-one-byte"
        single, follower, gate = self.held_responses(path)
        gate.set()
        byte = single.recv(2)
        self.assertEqual((len(byte), hashlib.sha256(byte).hexdigest()), self.client.expected(path, bounds=(7, 7)))
        self.assertEqual(single.recv(1), b"")
        tail = follower.read()
        self.assertEqual((len(tail), hashlib.sha256(tail).hexdigest()), self.client.expected(path, bounds=(8191, 8191)))
        drain(self.product)
        wait_for(lambda: all("end" in row for row in self.origin.records if row["path"] == path), "single-byte origin finished")
        before = self.origin.snapshot(path)
        self.assertEqual(before, {"requests": 2, "get": 1, "head": 1, "bytes": MIB})
        self.assertEqual([row["range"] for row in self.origin.records if row["path"] == path and row["method"] == "GET"], [f"bytes=0-{MIB - 1}"])
        self.checked_byte(path)
        self.assertEqual(self.origin.snapshot(path), before)

    def test_truncated_aligned_chunk_cannot_complete_or_cache_single_byte(self):
        path = "/large-one-byte-truncated"
        single, follower, gate = self.held_responses(path, truncated=True)
        gate.set()
        try:
            body = single.recv(2)
        except ConnectionResetError:
            body = b""
        self.assertEqual(body, b"", "truncated chunk completed the one-byte response")
        with self.assertRaises(http.client.IncompleteRead) as incomplete:
            follower.read()
        self.assertEqual(incomplete.exception.partial, b"")
        drain(self.product)
        wait_for(lambda: all("end" in row for row in self.origin.records if row["path"] == path), "truncated origin finished")
        failed = self.origin.snapshot(path)
        self.assertEqual(failed, {"requests": 2, "get": 1, "head": 1, "bytes": 32 << 10})

        self.origin.rules[path] = {}
        self.checked_byte(path)
        drain(self.product)
        rebuilt = self.origin.snapshot(path)
        self.assertEqual(rebuilt["get"], 2, "failed chunk was reused or retried within the original request")
        self.assertEqual(rebuilt["bytes"], MIB + (32 << 10))
        self.assertEqual([row["range"] for row in self.origin.records if row["path"] == path and row["method"] == "GET"],
                         [f"bytes=0-{MIB - 1}"] * 2)
        self.checked_byte(path)
        self.assertEqual(self.origin.snapshot(path), rebuilt)
