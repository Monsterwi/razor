"""Real-process recovery checks using the existing HTTP and cache lifecycle seams."""
from contextlib import ExitStack
import hashlib
import http.client
import os
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest

from acceptance import Client, MIB, Origin, Product, create_fixtures, wait_for
from public_acceptance import drain, purge_active_response


@unittest.skipUnless(os.environ.get("RAZOR_ACCEPTANCE_BINARY"), "set RAZOR_ACCEPTANCE_BINARY for real process recovery")
class PartialRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.cleanup = ExitStack()
        self.addCleanup(self.cleanup.close)
        self.root = Path(self.cleanup.enter_context(tempfile.TemporaryDirectory()))
        self.total = 3 * MIB + 37
        fixtures = create_fixtures(self.root / "fixtures", self.total)
        self.origin = Origin(fixtures, self.root / "origin.json")
        self.cleanup.callback(self.origin.close)
        self.client = Client(fixtures, self.root)
        self.cleanup.callback(self.client.raw.close)
        args = SimpleNamespace(binary=Path(os.environ["RAZOR_ACCEPTANCE_BINARY"]), cache_root=None,
            public_static=True, ram_algorithm="otter", ram_bytes=10 * MIB, block_bytes=MIB)
        self.product = Product("razor", self.root / "recovery", self.origin, args, capacity=64)
        self.cleanup.callback(self.product.stop)

    def checked(self, path, bounds=None, version=1):
        headers = {} if bounds is None else {"Range": f"bytes={bounds[0]}-{bounds[1]}"}
        row = self.client.checked(self.product, path, headers=headers, status=200 if bounds is None else 206,
                                  expected=self.client.expected(path, version=version, bounds=bounds))
        response = {name.lower(): value for name, value in row["headers"].items()}
        self.assertEqual(response["etag"], f'"large-v{version}"')
        self.assertEqual(response["content-length"], str(self.total if bounds is None else bounds[1] - bounds[0] + 1))
        self.assertEqual(response.get("content-range"), None if bounds is None else f"bytes {bounds[0]}-{bounds[1]}/{self.total}")
        # Recovery assertions observe completed cache state, not just client EOF.
        drain(self.product)
        return row

    def restart(self, crash=False):
        self.product.stop(crash=crash)
        self.product.start()

    def test_clean_restart_reuses_sparse_ranges_and_get_chunks(self):
        sparse, complete = "/large-sparse", "/large-complete"
        ranges = ((7, 23), (2 * MIB + 7, 2 * MIB + 23), (3 * MIB + 7, self.total - 1))
        for bounds in ranges:
            self.checked(sparse, bounds)
        self.assertEqual(self.origin.snapshot(sparse)["bytes"], 2 * MIB + 37)
        self.checked(complete)
        self.assertEqual(self.origin.snapshot(complete), {"requests": 1, "get": 1, "head": 0, "bytes": self.total})
        before = self.origin.snapshot()
        self.restart()
        for bounds in ranges:
            self.checked(sparse, bounds)
        self.checked(complete, (MIB - 17, MIB + 17))
        self.checked(complete)
        self.assertEqual(self.origin.snapshot(), before, "saved chunks must require no new HEAD or body")
        record_start = len(self.origin.records)
        self.checked(sparse)
        added = [row for row in self.origin.records[record_start:] if row["path"] == sparse]
        self.assertEqual([(row["method"], row["range"], row["bytes"]) for row in added],
                         [("GET", f"bytes={MIB}-{2 * MIB - 1}", MIB)])
        self.assertEqual(added[0]["headers"].get("If-Match"), '"large-v1"')
        before = self.origin.snapshot()
        self.checked(sparse, (MIB - 17, 2 * MIB + 17))
        self.assertEqual(self.origin.snapshot(), before)

    def test_kill_during_second_chunk_restarts_with_safe_miss(self):
        path, gate_at = "/large-interrupted", MIB + (128 << 10)
        entered, gate = threading.Event(), threading.Event()
        self.cleanup.callback(gate.set)
        self.origin.rules[path] = {"body_gate_at": gate_at, "entered": entered, "gate": gate}
        connection = http.client.HTTPConnection("127.0.0.1", self.product.port, timeout=5)
        self.cleanup.callback(connection.close)
        connection.request("GET", path, headers={"Host": "bench.local", "Accept-Encoding": "identity"})
        response = connection.getresponse()
        self.assertEqual(response.status, 200)
        self.assertEqual(response.getheader("Content-Length"), str(self.total))
        prefix = response.read(gate_at - 1)
        self.assertEqual((len(prefix), hashlib.sha256(prefix).hexdigest()), self.client.expected(path, bounds=(0, gate_at - 2)))
        self.assertTrue(entered.wait(5))
        self.assertFalse(gate.is_set())
        self.product.stop(crash=True)
        with self.assertRaises((http.client.IncompleteRead, OSError)):
            response.read()
        gate.set()
        wait_for(lambda: all("end" in row for row in self.origin.records if row["path"] == path), "killed origin connection closed")
        self.origin.rules[path] = {"version": 2}
        before = self.origin.snapshot(path)
        self.product.start()
        self.checked(path, version=2)
        after = self.origin.snapshot(path)
        self.assertEqual({name: after[name] - before[name] for name in after},
                         {"requests": 1, "get": 1, "head": 0, "bytes": self.total})
        before = self.origin.snapshot(path)
        self.checked(path, (MIB - 17, MIB + 17), version=2)
        self.assertEqual(self.origin.snapshot(path), before)

    def test_exact_and_prefix_purge_survive_clean_and_killed_restart(self):
        ranges = ((7, 23), (2 * MIB + 7, 2 * MIB + 23), (3 * MIB + 7, self.total - 1))
        for prefix in (False, True):
            for crash in (False, True):
                with self.subTest(prefix=prefix, crash=crash):
                    folder = f"/large-purge-recovery-{prefix}-{crash}/"
                    one, two = folder + "one", folder + "two"
                    keep = f"/large-outside-{prefix}-{crash}"
                    for path in (one, two):
                        for bounds in ranges:
                            self.checked(path, bounds)
                    self.checked(keep)
                    self.restart()  # Establish an older clean checkpoint before invalidating it.
                    self.client.checked(self.product, folder if prefix else one, method="PURGE",
                                        headers={"Authorization": "Bearer acceptance-local-token"}, expected=None)
                    affected = (one, two) if prefix else (one,)
                    for path in affected:
                        self.origin.rules[path] = {"version": 2}
                    self.restart(crash=crash)
                    for path in affected:
                        before = self.origin.snapshot(path)
                        for bounds in ranges:
                            self.checked(path, bounds, version=2)
                        after = self.origin.snapshot(path)
                        self.assertEqual(after["bytes"] - before["bytes"], 2 * MIB + 37)
                        self.assertEqual(after["get"] - before["get"], 3)
                    before = self.origin.snapshot(keep)
                    self.checked(keep)
                    if not crash:
                        self.assertEqual(self.origin.snapshot(keep), before, "clean PURGE restart lost another prefix")
                    if not prefix:
                        before = self.origin.snapshot(two)
                        for bounds in ranges:
                            self.checked(two, bounds)
                        if not crash:
                            self.assertEqual(self.origin.snapshot(two), before, "exact PURGE removed its adjacent URL")

    def test_late_private_stream_cannot_restore_purged_generation(self):
        for prefix in (False, True):
            for full in (False, True):
                with self.subTest(prefix=prefix, full=full):
                    result = purge_active_response(self.product, self.origin, self.client,
                                                   full=full, prefix=prefix, old_finishes_last=True)
                    path = result["path"]
                    new_get = next(row for row in self.origin.records if row["path"] == path
                                   and row["method"] == "GET" and not row["range"] and row["etag"] == '"large-v2"')
                    old_gets = [row for row in self.origin.records if row["path"] == path
                                and row["method"] == "GET" and row["etag"] == '"large-v1"']
                    self.assertTrue(old_gets and all(row["end"] > new_get["end"] for row in old_gets))
                    before = self.origin.snapshot(path)
                    self.restart()
                    self.checked(path, version=2)
                    self.checked(path, (MIB - 17, MIB + 17), version=2)
                    self.assertEqual(self.origin.snapshot(path), before, "late private chunks entered the saved generation")
