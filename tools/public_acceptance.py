"""Public-block acceptance and bounded mixed soak; reuses acceptance HTTP/process seams."""
import concurrent.futures
from contextlib import ExitStack
import hashlib
import http.client
import json
import math
import os
import signal
import socket
import threading
import time

from acceptance import CHUNK, MIB, Client, Origin, Product, save, wait_for

DRAIN_FIELDS = ("InFlight", "Queued", "CacheWriters", "CacheQueued", "RangeReaders", "RangeReaderQueued",
                "DiskQueued", "DiskBufferedBytes", "BufferReserved", "SharedFillTasks", "SharedFillWaiters", "RetiredRuntimes")


def snapshot(product):
    """Read only new log bytes; legacy stats remain available to older tools."""
    offset = getattr(product, "stats_offset", 0)
    requested = time.time_ns()
    product.proc.send_signal(signal.SIGUSR1)
    found = []
    def ready():
        nonlocal offset
        with (product.dir / "process.log").open() as stream:
            stream.seek(offset)
            for line in stream:
                marker = "HTTP resource stats JSON: "
                if marker in line:
                    sample = json.loads(line.split(marker, 1)[1])
                    if sample["SnapshotUnixNano"] >= requested:
                        found.append(sample)
            offset = stream.tell()
        return bool(found)
    wait_for(ready, "structured resource snapshot")
    product.stats_offset = offset
    return {"time": time.time(), "stats": found[-1], **product.resource()}


def drain(product):
    final = None
    def ready():
        nonlocal final
        final = snapshot(product)
        return all(final["stats"][field] == 0 for field in DRAIN_FIELDS) and final["stats"]["Origin"]["InFlight"] == 0
    wait_for(ready, "all task/waiter/writer/reader/buffer/origin resources drained", 20)
    return final


def record(output, evidence, name, **details):
    evidence.append({"name": name, "result": "PASS", **details})
    save(output / "public-functional.json", evidence)
    print("PASS " + name, flush=True)


def barrier_response(product, origin, client, path, full=False):
    block = product.args.block_bytes
    entered, gate = threading.Event(), threading.Event()
    origin.rules[path] = {"block_start": block, "entered": entered, "gate": gate}
    bounds = None if full else (0, 3 * block - 1)
    expected = client.expected(path, bounds=bounds)
    c = http.client.HTTPConnection("127.0.0.1", product.port, timeout=8)
    headers = {"Host": "bench.local", "Accept-Encoding": "identity"}
    if not full:
        headers["Range"] = f"bytes=0-{bounds[1]}"
    try:
        c.request("GET", path, headers=headers)
        response = c.getresponse()
        assert response.status == (200 if full else 206)
        prefix = response.read(block)
        assert len(prefix) == block and entered.wait(5), "first committed block or next producer missing"
        assert not gate.is_set(), "barrier unexpectedly released"
        digest, length = hashlib.sha256(prefix), len(prefix)
        assert digest.hexdigest() == client.expected(path, bounds=(0, block - 1))[1]
        gate.set()
        while True:
            body = response.read(CHUNK)
            if not body:
                break
            digest.update(body)
            length += len(body)
        assert (length, digest.hexdigest()) == expected, (length, expected)
        return {"status": response.status, "bytes": length, "sha256": digest.hexdigest(), "first_block_before_release": True}
    finally:
        gate.set()
        c.close()


def public_functional(args, fixtures, output, smoke=False):
    evidence = []
    with ExitStack() as cleanup:
        origin = Origin(fixtures, output / "origin-public.json")
        cleanup.callback(origin.close)
        client = Client(fixtures, output)
        cleanup.callback(client.raw.close)
        product = Product("razor", output / "public", origin, args)
        cleanup.callback(product.stop)
        block = args.block_bytes
        large = fixtures[("large", 1)].stat().st_size
        assert large >= 3 * block, "fixture must contain three blocks"
        for _ in range(2):
            client.checked(product, "/small-public")
        assert origin.snapshot("/small-public")["bytes"] == 256 << 10
        record(output, evidence, "256KiB whole-object public MISS/HIT")
        left, right = ((2, 1) if smoke else (16, 8))
        path = "/large-union"
        for start, end, expected_origin in ((0, left * MIB, left * MIB), (right * MIB, (left + right) * MIB, (left + right) * MIB)):
            bounds = (start, end - 1)
            client.checked(product, path, headers={"Range": f"bytes={start}-{end - 1}"}, status=206,
                           expected=client.expected(path, bounds=bounds))
            aligned = math.ceil(end / block) * block
            assert origin.snapshot(path)["bytes"] == min(large, aligned), origin.snapshot(path)
        assert origin.snapshot(path)["head"] == 1
        record(output, evidence, "overlap only aligned union fetched; HEAD separate", origin=origin.snapshot(path), block_bytes=block)
        # A later full GET must reuse the same blocks, then Range must be completely hot.
        client.checked(product, path)
        before = origin.snapshot(path)
        client.checked(product, path, headers={"Range": "bytes=0-16"}, status=206,
                       expected=client.expected(path, bounds=(0, 16)))
        assert origin.snapshot(path) == before and before["bytes"] == large
        record(output, evidence, "full GET and Range reuse identical blocks", origin=before)
        for full in (False, True):
            details = barrier_response(product, origin, client, "/large-barrier-" + str(full), full)
            record(output, evidence, "real HTTP committed first block before blocked second: " + ("GET" if full else "Range"), **details)
        for fault in ("truncate", "force_status"):
            path = "/large-body-error-" + fault
            origin.rules[path] = {"block_start": block, fault: True if fault == "truncate" else 200}
            row = client.request(product, path, headers={"Range": f"bytes=0-{3 * block - 1}"}, status=206,
                                 expected=client.expected(path, bounds=(0, 3 * block - 1)), case="expected-injected-body-error")
            assert row["status"] == 206 and row["error"] and block <= row["bytes"] < 3 * block, row
            record(output, evidence, "later " + fault + " detected by real body reader", response=row)
        # Gate one block, wait for 100 real TCP clients to subscribe before release.
        drain(product)
        time.sleep(1.1)  # Existing access logger samples per second; isolate this exact 100-log proof.
        path = "/large-hundred-ip"
        entered, gate = threading.Event(), threading.Event()
        origin.rules[path] = {"block_start": 0, "entered": entered, "gate": gate}
        count = 100
        limits = (0, block - 1)
        expected = client.expected(path, bounds=limits)
        def request(index):
            return client.checked(product, path, source_ip=f"127.0.0.{index + 2}",
                headers={"Range": f"bytes=0-{block - 1}", "User-Agent": f"client-{index}",
                         "Traceparent": f"00-{index + 1:032x}-{index + 1:016x}-01", "X-Request-Id": f"trace-{index}"},
                status=206, expected=expected, case="100-real-source-IP")
        with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
            futures = [pool.submit(request, index) for index in range(count)]
            try:
                assert entered.wait(5)
                wait_for(lambda: snapshot(product)["stats"]["SharedFillWaiters"] == count, "100 subscribed TCP clients", 12)
            finally:
                gate.set()
            rows = [future.result() for future in futures]
        assert origin.snapshot(path)["get"] == 1 and origin.snapshot(path)["bytes"] == block, origin.snapshot(path)
        headers = [r["headers"] for r in origin.records if r["path"] == path]
        for h in headers:
            lower = {key.lower(): value for key, value in h.items()}
            assert lower.get("user-agent") == "Razor" and not any(key in lower for key in
                ("forwarded", "x-forwarded-for", "x-real-ip", "traceparent", "x-request-id")), h
        addresses = set()
        def logged():
            nonlocal addresses
            access = [json.loads(line) for line in (product.dir / "access.log").read_text().splitlines()]
            addresses = {row["remote_addr"].rsplit(":", 1)[0] for row in access if row.get("path") == path}
            return len(addresses) == count
        wait_for(logged, "100 completed access records")
        record(output, evidence, "100 distinct TCP IP/UA/trace clients share one block producer", clients=len(rows),
               source_ips=sorted(addresses), origin=origin.snapshot(path))
        # Unknown headers preserve independent tasks; write ownership rejects the contender.
        path = "/large-unknown"
        entered, gate = threading.Event(), threading.Event()
        origin.rules[path] = {"block_start": 0, "entered": entered, "gate": gate}
        with concurrent.futures.ThreadPoolExecutor() as pool:
            owner = pool.submit(client.checked, product, path, headers={"Range": f"bytes=0-{block - 1}", "X-Unknown": "a"},
                                expected=expected, status=206)
            try:
                assert entered.wait(5)
                other = client.request(product, path, headers={"Range": f"bytes=0-{block - 1}", "X-Unknown": "b"}, status=None,
                                       expected=expected, case="expected-unknown-contention")
                assert other["status"] >= 500 and origin.snapshot(path)["get"] == 1, other
            finally:
                gate.set()
            owner.result()
        record(output, evidence, "unknown headers do not join or create duplicate active producer")
        # Last subscriber cancellation while the next producer is blocked.
        path = "/large-last-cancel"
        entered, gate = threading.Event(), threading.Event()
        origin.rules[path] = {"block_start": block, "entered": entered, "gate": gate}
        c = http.client.HTTPConnection("127.0.0.1", product.port, timeout=8)
        try:
            c.request("GET", path, headers={"Host": "bench.local", "Accept-Encoding": "identity", "Range": f"bytes=0-{3 * block - 1}"})
            response = c.getresponse()
            prefix = response.read(block)
            assert len(prefix) == block and entered.wait(5)
            c.sock.shutdown(socket.SHUT_RDWR)
            response.close()
            c.close()
        finally:
            gate.set()
            c.close()
        final = drain(product)
        assert final["stats"]["HeapAlloc"] > 0 and "GCCount" in final["stats"]
        assert origin.snapshot(path)["get"] <= 2, origin.snapshot(path)
        record(output, evidence, "reader cancellation bounds prefetch and drains", drain=final)
        # Clean restart reuses committed blocks; PURGE forces a new generation.
        path = "/large-union"
        before = origin.snapshot(path)
        product.stop()
        product.start()
        product.stats_offset = 0
        client.checked(product, path)
        assert origin.snapshot(path) == before
        client.checked(product, path, method="PURGE", headers={"Authorization": "Bearer acceptance-local-token"}, expected=None)
        client.checked(product, path, headers={"Range": f"bytes=0-{block - 1}"}, status=206, expected=expected)
        assert origin.snapshot(path)["bytes"] - before["bytes"] == block
        record(output, evidence, "restart keeps committed blocks and PURGE refills exact one block")
        product.reload({"http.health.interval": ("INT", 1), "http.health.failures": ("INT", 1), "http.health.successes": ("INT", 1)})
        # Establish the block through the current runtime before removing the origin.
        client.checked(product, path, headers={"Range": f"bytes=0-{block - 1}"}, status=206, expected=expected)
        origin.healthy = False
        wait_for(lambda: client.request(product, "/small-health-probe", headers={"Authorization": "Bearer probe"}, status=None)["status"] == 502,
                 "all origins unhealthy", 10)
        before = origin.snapshot(path)
        client.checked(product, path, headers={"Range": f"bytes=0-{block - 1}"}, status=206, expected=expected)
        assert origin.snapshot(path) == before
        missing = client.request(product, path, headers={"Range": f"bytes={2 * block}-{3 * block - 1}"}, status=None)
        assert missing["status"] >= 500, missing
        record(output, evidence, "offline fresh range zero origin; missing block controlled failure", origin=before)
        origin.healthy = True
        wait_for(lambda: client.request(product, "/small-health-probe", headers={"Authorization": "Bearer probe"}, status=None)["status"] == 200,
                 "origin recovered", 10)
        # Reopening the same fixed geometry must retain the committed chunk.
        for bounds in ((0, 16), (block - 17, block - 1)):
            product.stop()
            product.start()
            product.stats_offset = 0
            before = origin.snapshot(path)
            response = client.checked(product, path, headers={"Range": f"bytes={bounds[0]}-{bounds[1]}"},
                                      status=206, expected=client.expected(path, bounds=bounds))
            headers = {name.lower(): value for name, value in response["headers"].items()}
            assert headers["content-length"] == "17", response
            assert headers["content-range"] == f"bytes {bounds[0]}-{bounds[1]}/{large}", response
            assert origin.snapshot(path) == before
        record(output, evidence, "fixed 1 MiB chunk survives repeated clean restart", drain=drain(product))
    return evidence


def mixed_soak(args, fixtures, output):
    """Bounded working set, real validated traffic for the whole requested duration."""
    assert args.soak_seconds >= 7200 or args.allow_short_soak, "each policy requires >=7200 seconds; --allow-short-soak is smoke only"
    with ExitStack() as cleanup:
        origin = Origin(fixtures, output / "origin-soak-tail.json")
        cleanup.callback(origin.close)
        client = Client(fixtures, output)
        cleanup.callback(client.raw.close)
        product = Product("razor", output / "soak", origin, args, capacity=512)
        cleanup.callback(product.stop)
        initial = snapshot(product)
        start = time.monotonic()
        started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        save(output / "progress.json", {"pid": os.getpid(), "server_pid": product.proc.pid, "started_utc": started,
             "algorithm": args.ram_algorithm, "target_seconds": args.soak_seconds, "stages": 0, "requests": 0, "success_so_far": True})
        large = fixtures[("large", 1)].stat().st_size
        stages = 0
        requests = 0
        origin_total = {"requests": 0, "get": 0, "head": 0, "bytes": 0}
        # Only 80 small keys + two large objects; cold keys are explicitly PURGEd and reused.
        origin.rules["/large-soak-slow"] = {"chunk_delay": .001}
        with (output / "origin-soak.jsonl").open("a") as origin_raw, (output / "soak-stages.jsonl").open("a") as stage_raw:
            while time.monotonic() - start < args.soak_seconds or stages < 7:
                phase = stages % 7
                name = ("hotspot", "longtail", "range-drag", "range-overlap", "fullGET", "slow-origin-client", "cold-miss")[phase]
                before = snapshot(product)
                if phase == 0:
                    summary = client.batch(product, origin, name, "/small-soak-hot", 8, count=4)
                elif phase == 1:
                    summary = client.batch(product, origin, name, "/small-soak-tail-0", 4, count=8,
                        paths=[f"/small-soak-tail-{index}" for index in range(64)])
                elif phase in (2, 3):
                    width = min(4 * MIB, large // 4)
                    stride = min(MIB, width)
                    slots = max(1, (large - width - 3 * stride) // MIB + 1)
                    base = ((stages * 7) % slots) * MIB if phase == 2 else 0
                    ranges = [(base + index * stride, base + index * stride + width - 1) for index in range(4)]
                    summary = client.batch(product, origin, name, "/large-soak-main", 4, count=2, ranges=ranges)
                elif phase == 4:
                    summary = client.batch(product, origin, name, "/large-soak-main", 2, count=1)
                elif phase == 5:
                    client.checked(product, "/large-soak-slow", method="PURGE", headers={"Authorization": "Bearer acceptance-local-token"}, expected=None)
                    width = min(4 * MIB, large // 2)
                    summary = client.batch(product, origin, name, "/large-soak-slow", 2, count=1, slow=.001,
                        ranges=[(0, width - 1), (width // 2, width // 2 + width - 1)])
                else:
                    path = f"/small-soak-cold-{(stages // 7) % 16}"
                    client.checked(product, path, method="PURGE", headers={"Authorization": "Bearer acceptance-local-token"}, expected=None)
                    summary = client.batch(product, origin, name, path, 4, count=1)
                    assert summary["origin"]["get"] == 1, summary
                after = snapshot(product)
                stage = {"stage": stages, "phase": name, "elapsed_seconds": time.monotonic() - start,
                         "before": before, "after": after, "batch": summary}
                stage_raw.write(json.dumps(stage) + "\n")
                stage_raw.flush()
                requests += summary["requests"]
                stages += 1
                save(output / "progress.json", {"pid": os.getpid(), "server_pid": product.proc.pid, "started_utc": started, "algorithm": args.ram_algorithm,
                     "target_seconds": args.soak_seconds, "elapsed_seconds": time.monotonic() - start, "stages": stages, "requests": requests,
                     "last_phase": name, "success_so_far": True})
                # Bound harness memory; every origin request remains in the append-only raw file.
                with origin.lock:
                    for row in origin.records:
                        if row["path"] != "/health":
                            origin_raw.write(json.dumps(row) + "\n")
                            origin_total["requests"] += 1
                            origin_total["get" if row["method"] == "GET" else "head"] += 1
                            origin_total["bytes"] += row["bytes"]
                    origin.records.clear()
                origin_raw.flush()
                client.summaries.clear()
                client.hashes = {key: value for key, value in client.hashes.items() if key[0] == "small"}
        final = drain(product)
        result = {"success": True, "started_utc": started, "ended_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                  "elapsed_seconds": time.monotonic() - start, "target_seconds": args.soak_seconds,
                  "qualifies_two_hour_soak": args.soak_seconds >= 7200 and time.monotonic() - start >= 7200,
                  "algorithm": args.ram_algorithm, "stages": stages, "requests": requests, "origin": origin_total,
                  "initial": initial, "final": final, "drain_fields": list(DRAIN_FIELDS)}
        assert requests > 0 and stages >= 1
        save(output / "soak-outcome.json", result)
        return result


def counter_metrics(before, after):
    """Logical demand counters and RAM storage lookups have distinct denominators."""
    fields = ("Requests", "Hits", "Misses", "Bypass", "Bytes", "RangeBlockHits", "RangeBlockMisses", "RangeHitBytes", "RangeMissBytes",
              "SharedFillJoined", "SharedFillCompleted", "SharedFillFailed", "Cancelled", "GCCount", "GCPauseTotalNanoseconds", "TotalAlloc")
    delta = {key: after[key] - before[key] for key in fields}
    ram = {}
    for field in ("Hits", "Misses", "Evictions", "EvictionWeight", "Promotions", "GhostHits"):
        ram[field] = sum(row.get(field, 0) for row in after.get("RAM") or []) - sum(row.get(field, 0) for row in before.get("RAM") or [])
    def rate(hits, misses):
        return hits / (hits + misses) if hits + misses else None
    return {"delta": delta, "ram_lookup_delta": ram,
            "request_hit_fraction": rate(delta["Hits"], delta["Misses"]),
            "block_hit_fraction": rate(delta["RangeBlockHits"], delta["RangeBlockMisses"]),
            "logical_byte_hit_fraction": rate(delta["RangeHitBytes"], delta["RangeMissBytes"]),
            "ram_lookup_hit_fraction": rate(ram["Hits"], ram["Misses"])}


def public_benchmark(args, fixtures, output):
    """Sequential pressure groups, every scenario repeated; no competing hey job."""
    with ExitStack() as cleanup:
        origin = Origin(fixtures, output / "origin-public-benchmark.json")
        cleanup.callback(origin.close)
        client = Client(fixtures, output)
        cleanup.callback(client.raw.close)
        product = Product("razor", output / "public-benchmark", origin, args)
        cleanup.callback(product.stop)
        for repeat in range(args.repeats):
            for size, steps in (("small", (1, 8, 32, 100)), ("large", (1, 8, 32))):
                hot = f"/{size}-hot-public"
                client.checked(product, hot)
                for concurrency in steps:
                    row = client.batch(product, origin, f"{size}-hot-c{concurrency}-r{repeat + 1}", hot, concurrency,
                                       count=1, duration=args.seconds)
                    assert row["origin"]["bytes"] == 0, row
                for concurrency in (1, 8, 32):
                    path = f"/{size}-cold-c{concurrency}-r{repeat + 1}"
                    entered, gate = threading.Event(), threading.Event()
                    origin.rules[path] = {"block_start": 0, "entered": entered, "gate": gate}
                    # Gate only the first GET: no artificial per-block RTT on a 100MiB object.
                    def release():
                        assert entered.wait(10), "cold first body not entered"
                        if concurrency > 1:
                            wait_for(lambda: snapshot(product)["stats"]["SharedFillWaiters"] == concurrency,
                                     "cold concurrent block subscribers", 10)
                        gate.set()
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                        future = pool.submit(release)
                        try:
                            row = client.batch(product, origin, f"{size}-cold-c{concurrency}-r{repeat + 1}", path, concurrency, count=1)
                        finally:
                            gate.set()
                        future.result()
                    assert row["origin"]["bytes"] == fixtures[(size, 1)].stat().st_size, row
            large = fixtures[("large", 1)].stat().st_size
            width = min(16 * MIB, large // 2)
            ranges = [(index * (width // 4), index * (width // 4) + width - 1) for index in range(4)]
            client.batch(product, origin, f"overlap-r{repeat + 1}", f"/large-overlap-load-{repeat}", 8, count=1, ranges=ranges)
            width = min(4 * MIB, large // 4)
            slots = max(1, (large - width) // MIB + 1)
            dragged = [((index * 7) % slots * MIB, (index * 7) % slots * MIB + width - 1) for index in range(12)]
            client.batch(product, origin, f"drag-r{repeat + 1}", f"/large-drag-load-{repeat}", 8, count=3, ranges=dragged)
            slow = f"/large-slow-load-{repeat}"
            origin.rules[slow] = {"header_delay": .002, "chunk_delay": .001}
            client.batch(product, origin, f"slow-origin-r{repeat + 1}", slow, 8, count=1)
            client.batch(product, origin, f"slow-client-r{repeat + 1}", slow, 8, count=1, slow=.001)
            paths = ["/small-hot-mixed"] * 9 + [f"/small-tail-mixed-{index}" for index in range(7)] + ["/large-mixed-a", "/large-mixed-b"] * 2
            for path in set(paths):
                client.checked(product, path)
            client.batch(product, origin, f"mixed-r{repeat + 1}", paths[0], 8, count=1, duration=args.seconds, paths=paths)
            client.batch(product, origin, f"longtail-r{repeat + 1}", f"/small-unique-r{repeat}", 8, count=8, unique_paths=True)
        save(output / "benchmark-drain.json", drain(product))
