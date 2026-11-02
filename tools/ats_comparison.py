#!/usr/bin/env python3
"""Sequential Razor / ATS 10.x comparison. New output and cache directories only.

Uses acceptance.py's real origin, body hashes, process lifecycle and /proc sampler.
ATS must be installed separately. Never drops global page cache or deletes caches.
"""
import argparse
import concurrent.futures
from contextlib import ExitStack
import json
import os
from pathlib import Path
import pwd
import random
import subprocess
import threading
import time

from acceptance import Client, MIB, Origin, Product, device_io, file_hash, free_port, save
from hey_benchmark import TEMPLATE, checked_summary


def ats_records(port):
    # JSON is valid YAML; dotted record names are supported in records.yaml.
    return {"records": {
        "admin.user_id": "nobody", "http.server_ports": f"{port}:ip-in=127.0.0.1",
        "exec_thread.autoconfig.enabled": 0, "exec_thread.limit": 8,
        "cache.ram_cache.size": 10 * MIB, "cache.ram_cache.algorithm": 1,
        "cache.max_doc_size": 0, "cache.enable_read_while_writer": 1,
        "http.background_fill_active_timeout": 0, "http.background_fill_completed_threshold": 0.0,
        "http.cache.open_write_fail_action": 5, "http.cache.max_open_read_retries": 1000, "http.cache.max_open_write_retries": 10,
        "http.cache.open_read_retry_time": 10, "http.cache.ignore_client_no_cache": 0,
        "http.connect_attempts_max_retries": 0, "http.connect_attempts_rr_retries": 0, "http.connect_attempts_max_retries_suspect_server": 0,
        "http.transaction_no_activity_timeout_in": 120, "http.transaction_no_activity_timeout_out": 120,
        "log.logging_enabled": 3,
    }}


def ats_remap(origin, variant, prefetch=1):
    plugins = "@plugin=cachekey.so @pparam=--sort-params=true"
    if variant == "ats-slice":
        plugins = (f"@plugin=slice.so @pparam=--blockbytes=1048576 @pparam=--prefetch-count={prefetch} " + plugins +
                   " @pparam=--include-headers=Range @plugin=cache_range_requests.so"
                   " @pparam=--no-modify-cachekey @pparam=--consider-ident @pparam=--verify-cacheability")
    return f"map http://bench.local/ {origin}/ {plugins}\n"


class ATS10(Product):
    def __init__(self, directory, origin, args, variant):
        self.kind, self.dir, self.args = "ats", directory, args
        self.port, self.proc = free_port(), None
        self.env = dict(os.environ, LD_LIBRARY_PATH=f"{args.ats_env}/lib:{args.ats_prefix}/lib")
        subprocess.run([str(args.ats_prefix / "bin/traffic_layout"), "init", f"--path={directory}", "--copy-style=FULL"],
                       env=self.env, check=True, stdout=subprocess.DEVNULL)
        layout = directory / "runroot.yaml"
        runtime = args.ats_prefix.parent / "run" / f"{directory.parent.name}-{self.port}"
        runtime.mkdir(parents=True, exist_ok=False)
        layout.write_text("\n".join("runtimedir: " + str(runtime) if line.startswith("runtimedir:") else line
                                    for line in layout.read_text().splitlines()) + "\n")
        # ATS runroot keeps the installed layout relative to the new root.
        config = next(directory.rglob("records.yaml")).parent
        (config / "records.yaml").write_text(json.dumps(ats_records(self.port), indent=2) + "\n")
        (config / "remap.config").write_text(ats_remap(origin.url, variant, args.ats_prefetch))
        self.cache_dir = args.cache_root / directory.name
        self.cache_dir.mkdir(parents=True, exist_ok=False)
        (config / "storage.config").write_text(str(self.cache_dir) + " 1024M\n")
        (config / "logging.yaml").write_text(json.dumps({"logging": {
            "formats": [{"name": "benchfmt", "format": "%<cqts> %<cqhm> %<pqu> %<pssc> %<pscl> %<crc> %<ttms>"}],
            "logs": [{"mode": "ascii", "filename": "bench", "format": "benchfmt"}]}}) + "\n")
        self.command = [str(args.ats_prefix / "bin/traffic_server"), f"--run-root={directory}"]
        nobody = pwd.getpwnam("nobody")
        for base in (directory, self.cache_dir, runtime):
            for root, dirs, files in os.walk(base):
                os.chown(root, nobody.pw_uid, nobody.pw_gid)
                os.chmod(root, 0o755)
                for name in files:
                    os.chown(Path(root) / name, nobody.pw_uid, nobody.pw_gid)
        self.start()
        command = [str(args.ats_prefix / "bin/traffic_ctl"), f"--run-root={directory}", "config", "get",
                   "proxy.config.exec_thread.limit", "proxy.config.exec_thread.autoconfig.enabled",
                   "proxy.config.cache.ram_cache.size", "proxy.config.cache.enable_read_while_writer",
                   "proxy.config.http.cache.open_write_fail_action"]
        try:
            runtime_config = subprocess.check_output(command, env=self.env, text=True, stderr=subprocess.STDOUT)
            (directory / "runtime-config.txt").write_text(runtime_config)
        except BaseException:
            self.stop()
            raise

    def resource(self):
        row = super().resource()
        row["cache_device"] = device_io(self.cache_dir)
        return row


def delta(before, after):
    return {key: after[key] - before[key] for key in before}


def hot(client, product, origin, args, size, concurrency, repeat):
    path = f"/{size}-hot"
    client.checked(product, path)
    name = f"{size}-hot-c{concurrency}-r{repeat}"
    before, initial = origin.snapshot(), product.resource()
    samples, done = [initial], threading.Event()
    def sample():
        while not done.wait(.1):
            samples.append(product.resource())
    monitor = threading.Thread(target=sample)
    monitor.start()
    command = [str(args.hey), "-z", f"{args.seconds}s", "-c", str(concurrency), "-t", "120", "-cpus", "8",
               "-host", "bench.local", "-H", "Accept-Encoding: identity", "-disable-compression", "-o", TEMPLATE,
               f"http://127.0.0.1:{product.port}{path}"]
    try:
        raw = json.loads(subprocess.check_output(command, text=True, timeout=args.seconds + 180))
    finally:
        done.set()
        monitor.join()
    samples.append(product.resource())
    save(client.output / (name + ".hey.json"), raw)
    row = checked_summary(raw, client.expected(path)[0])
    row.update(name=name, concurrency=concurrency, repeat=repeat, resource_samples=samples,
               origin=delta(before, origin.snapshot()),
               process_io_bytes={k: samples[-1][k] - initial[k] for k in ("read_bytes", "write_bytes")},
               cpu_seconds_per_gib=(samples[-1]["cpu_seconds"] - initial["cpu_seconds"]) / (raw["bytes"] / (1 << 30)),
               rss_peak_bytes=max(s["rss_bytes"] for s in samples), fd_peak=max(s["fds"] for s in samples), command=command)
    client.checked(product, path)
    assert origin.snapshot() == before, (name, "warm load unexpectedly fetched origin", before, origin.snapshot())
    return row


def correctness(client, product, origin, args):
    checks = []
    def check(name, run):
        try:
            value = run()
            checks.append({"name": name, "passed": True, "details": value})
        except AssertionError as error:
            checks.append({"name": name, "passed": False, "error": str(error)[:3000]})
        save(client.output / "functional.json", checks)
        print(name, checks[-1]["passed"], flush=True)
    for size in ("small", "large"):
        path = f"/{size}-correctness"
        def miss_hit(path=path):
            client.checked(product, path)
            before = origin.snapshot(path)
            client.checked(product, path)
            assert origin.snapshot(path) == before, (before, origin.snapshot(path))
            return {"origin": before, "body": client.expected(path)}
        check(size + " MISS/HIT SHA256", miss_hit)
    def query():
        client.checked(product, "/small-query?b=2&a=1")
        before = origin.snapshot()
        client.checked(product, "/small-query?a=1&b=2")
        assert origin.snapshot() == before
        return before
    check("query order shares cache", query)
    def cross_client():
        path = "/small-cross-client"
        origin.rules[path] = {"header_delay": .3}
        barrier = threading.Barrier(8)
        def request(index):
            barrier.wait()
            return client.checked(product, path, headers={"User-Agent": f"cdn-client-{index}"},
                                  source_ip=f"127.0.0.{index+2}", case="cross-client")
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            rows = list(pool.map(request, range(8)))
        summary = origin.snapshot(path)
        assert summary["get"] == 1, summary
        return {"clients": len(rows), "origin": summary}
    check("eight IP and UA clients share cold fill", cross_client)
    ranges = [(i * 8 * MIB, (i * 8 + 16) * MIB - 1) for i in range(2)]
    for repeat in range(args.repeats):
        for size, concurrency in (("small", 100), ("large", 16)):
            path = f"/{size}-cold-r{repeat+1}"
            origin.rules[path] = {"header_delay": .2 if size == "small" else .005}
            check(f"{size} cold burst r{repeat+1}", lambda path=path, concurrency=concurrency, size=size:
                  client.batch(product, origin, f"{size}-cold-r{repeat+1}", path, concurrency, count=1))
    path = "/large-overlap"
    check("overlapping Range cold", lambda: client.batch(product, origin, "overlap-cold", path, 8, count=1, ranges=ranges))
    check("overlapping Range warm", lambda: client.batch(product, origin, "overlap-warm", path, 8, count=1, ranges=ranges))
    origin.rules[path] = {"force_status": 503}
    check("cached Range while origin fails", lambda: client.batch(product, origin, "range-offline", path, 8, count=1, ranges=ranges))
    starts = random.Random(193).sample(range(0, 98 * MIB, MIB // 2), 32)
    seeks = [(start, start + MIB - 1) for start in starts]
    check("random nonaligned Range", lambda: client.batch(product, origin, "random-seek", "/large-seek", 16, count=2, ranges=seeks))
    # RWW: later origin block is held; verify first body byte arrives before releasing it.
    def rww():
        import http.client
        gate, entered = threading.Event(), threading.Event()
        path = "/large-rww"
        if product.kind == "razor":
            origin.rules[path] = {"body_gate_at": MIB, "gate": gate, "entered": entered}
        elif args.variant == "ats-slice":
            origin.rules[path] = {"block_start": MIB, "gate": gate, "entered": entered}
        else:
            origin.rules[path] = {"chunk_delay": .002}
        c = http.client.HTTPConnection("127.0.0.1", product.port, timeout=15)
        start = time.monotonic()
        try:
            c.request("GET", path, headers={"Host": "bench.local", "Accept-Encoding": "identity"})
            response = c.getresponse()
            first = response.read(1)
            elapsed = time.monotonic() - start
            first_origin = origin.snapshot(path)
            gate.set()
            import hashlib
            digest, length = hashlib.sha256(first), len(first)
            while True:
                block = response.read(64 << 10)
                if not block:
                    break
                digest.update(block)
                length += len(block)
            assert response.status == 200 and (length, digest.hexdigest()) == client.expected(path)
            assert first and first_origin["bytes"] < client.expected(path)[0], first_origin
            return {"ttfb_ms": elapsed * 1000, "origin_at_first_byte": first_origin, "origin_final": origin.snapshot(path)}
        finally:
            gate.set()
            c.close()
    check("RWW before full object complete", rww)
    def no_store():
        path = "/small-no-store"
        origin.rules[path] = {"cache_control": "no-store"}
        client.checked(product, path)
        client.checked(product, path)
        assert origin.snapshot(path)["get"] == 2, origin.snapshot(path)
        return origin.snapshot(path)
    check("no-store bypass", no_store)
    def truncated():
        path = "/large-truncated"
        rule = {"truncate": True}
        if product.kind == "razor":
            rule = {"truncate_at": MIB + (64 << 10)}
        elif args.variant == "ats-slice":
            rule["block_start"] = MIB
        origin.rules[path] = rule
        row = client.request(product, path, expected=client.expected(path), case="truncated")
        assert row["error"], row
        origin.rules[path] = {}
        client.checked(product, path)
        return {"failed_status": row["status"], "failed_bytes": row["bytes"], "error": row["error"],
                "retry_sha256_passed": True, "origin": origin.snapshot(path)}
    check("truncated origin fails then retries exact body", truncated)
    def restart():
        before = origin.snapshot()
        product.stop()
        product.start()
        client.checked(product, "/small-correctness")
        client.checked(product, "/large-correctness")
        assert origin.snapshot() == before, (before, origin.snapshot())
        return {"origin_unchanged": True}
    check("clean restart cached SHA256", restart)
    return checks


def physical_reads(client, product, origin, args):
    path = "/large-physical"
    client.checked(product, path)
    results = []
    for repeat in range(1, args.repeats + 1):
        product.stop()
        product.start()
        files = [product.cache_file] if product.kind == "razor" else list(product.cache_dir.rglob("*"))
        for path_on_disk in files:
            if path_on_disk.is_file():
                with path_on_disk.open("rb") as f:
                    os.fsync(f.fileno())
                    os.posix_fadvise(f.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
        before, initial = origin.snapshot(), product.resource()
        command = [str(args.hey), "-n", "1", "-c", "1", "-t", "120", "-cpus", "1", "-host", "bench.local",
                   "-H", "Accept-Encoding: identity", "-disable-compression", "-o", TEMPLATE,
                   f"http://127.0.0.1:{product.port}{path}"]
        raw = json.loads(subprocess.check_output(command, text=True, timeout=180))
        final = product.resource()
        row = checked_summary(raw, client.expected(path)[0])
        row.update(repeat=repeat, origin=delta(before, origin.snapshot()),
                   process_io_bytes={k: final[k]-initial[k] for k in ("read_bytes", "write_bytes")},
                   cache_device_io_delta=None)
        row["physical_read_verified"] = row["process_io_bytes"]["read_bytes"] >= client.expected(path)[0] * .99
        if initial["cache_device"] and final["cache_device"]:
            row["cache_device_io_delta"] = {k: final["cache_device"][k]-initial["cache_device"][k]
                                            for k in ("read_bytes", "write_bytes", "io_ms")}
        client.checked(product, path)
        assert origin.snapshot() == before, ("physical request unexpectedly fetched origin", before, origin.snapshot())
        results.append(row)
        save(client.output / "physical.json", results)
        print("physical", repeat, row["physical_read_verified"], round(row["mib_per_second"],2), flush=True)
    return results


def mixed_paths():
    # 82% small / 18% large request slots; 2GiB unique payload exceeds the 1GiB cache.
    return ([f"/small-mixed-{i}" for i in range(16) for _ in range(16)] +
            [f"/large-popular-{i}" for i in range(4) for _ in range(10)] +
            [f"/large-tail-{i}" for i in range(16)])


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for key in ("binary", "hey", "fixtures", "output", "cache-root", "ats-prefix", "ats-env"):
        p.add_argument("--" + key, type=Path, required=True)
    p.add_argument("--variant", choices=("razor", "ats-native", "ats-slice"), required=True)
    p.add_argument("--source-commit", required=True)
    p.add_argument("--seconds", type=int, default=10)
    p.add_argument("--repeats", type=int, default=3)
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--smoke", action="store_true")
    mode.add_argument("--compat-only", action="store_true")
    mode.add_argument("--mixed-only", action="store_true")
    mode.add_argument("--cold-only", action="store_true")
    p.add_argument("--ats-prefetch", type=int, choices=(0, 1), default=1)
    args = p.parse_args()
    assert args.seconds > 0 and args.repeats > 0
    args.output.mkdir(parents=True, exist_ok=False)
    args.cache_root.mkdir(parents=True, exist_ok=False)
    args.public_static, args.block_bytes, args.ram_algorithm, args.ram_bytes = True, MIB, "otter", 10 * MIB
    fixtures = {(size, v): args.fixtures / f"{size}-v{v}.bin" for size in ("small", "large") for v in (1, 2)}
    assert all(fixtures[("small", v)].stat().st_size == 256 << 10 for v in (1, 2)), "expected 256KiB small fixtures"
    large_bytes = fixtures[("large", 1)].stat().st_size
    assert large_bytes in (100 * MIB, 200 * MIB) and fixtures[("large", 2)].stat().st_size == large_bytes
    assert not args.mixed_only or large_bytes == 100 * MIB, "mixed workload requires 100MiB fixtures"
    hey_meta = json.loads(args.hey.with_suffix(".build.json").read_text())
    assert hey_meta["variant"] == "razor-bodycheck" and hey_meta["sha256"] == file_hash(args.hey)
    save(args.output / "environment.json", {"arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
         "source_commit": args.source_commit, "binary_sha256": file_hash(args.binary), "harness_sha256": file_hash(Path(__file__)), "hey_build": hey_meta,
         "fixture_sha256": {path.name: file_hash(path) for path in fixtures.values()}, "uname": list(os.uname()),
         "start_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
         "method": "HTTP/1.1 loopback keepalive; no TLS; 8 server threads/GOMAXPROCS, hey cpus=8; sequential loads"})
    with ExitStack() as cleanup:
        origin = Origin(fixtures, args.output / "origin.json")
        cleanup.callback(origin.close)
        client = Client(fixtures, args.output)
        cleanup.callback(client.raw.close)
        product = (Product("razor", args.output / "server", origin, args) if args.variant == "razor" else
                   ATS10(args.output / "server", origin, args, args.variant))
        cleanup.callback(product.stop)
        if args.smoke:
            for size in ("small", "large"):
                path = "/" + size + "-smoke"
                client.checked(product, path)
                client.checked(product, path)
            save(args.output / "outcome.json", {"smoke": True, "success": True, "origin": origin.snapshot()})
            return
        if args.compat_only:
            client.checked(product, "/large-200-compat")
            before = origin.snapshot()
            client.checked(product, "/large-200-compat")
            bounds = (MIB // 2, 2 * MIB - 1)
            client.checked(product, "/large-200-compat", headers={"Range": "bytes=%d-%d" % bounds}, status=206,
                           expected=client.expected("/large-200-compat", bounds=bounds))
            assert origin.snapshot() == before
            save(args.output / "outcome.json", {"success": True, "compat_bytes": client.expected("/large-200-compat")[0], "origin": before})
            return
        if args.cold_only:
            for repeat in range(1, args.repeats + 1):
                path = f"/large-cold-r{repeat}"
                origin.rules[path] = {"header_delay": .005}
                client.batch(product, origin, f"large-cold-r{repeat}", path, 16, count=1)
            ranges = [(i * 8 * MIB, (i * 8 + 16) * MIB - 1) for i in range(2)]
            client.batch(product, origin, "overlap-cold", "/large-overlap", 8, count=1, ranges=ranges)
            client.batch(product, origin, "overlap-warm", "/large-overlap", 8, count=1, ranges=ranges)
            time.sleep(2)
            origin.rules["/large-overlap"] = {"force_status": 503}
            offline_passed = True
            try:
                client.batch(product, origin, "range-offline-after-2s", "/large-overlap", 8, count=1, ranges=ranges)
            except AssertionError:
                offline_passed = False
            save(args.output / "outcome.json", {"success": True, "cold_batches": args.repeats + 3,
                 "ats_prefetch": args.ats_prefetch, "offline_after_2s_passed": offline_passed})
            return
        if args.mixed_only:
            paths = mixed_paths()
            for path in dict.fromkeys(p for p in paths if "tail" not in p):
                client.checked(product, path)
            for repeat in range(1, args.repeats + 1):
                client.batch(product, origin, f"mixed-churn-r{repeat}", "/large-mixed", 8,
                             duration=15, paths=paths)
            save(args.output / "outcome.json", {"success": True, "mixed_batches": args.repeats,
                 "scheduled_small_slots": 256, "scheduled_large_slots": 56,
                 "unique_payload_bytes": 16 * (256 << 10) + 20 * 100 * MIB,
                 "cache_bytes": 1024 * MIB, "note": "actual request mix recorded per batch; seeded per-worker shuffle"})
            return
        checks = correctness(client, product, origin, args)
        results = []
        for size, levels in (("small", (32, 100)), ("large", (8, 32))):
            for concurrency in levels:
                for repeat in range(1, args.repeats + 1):
                    row = hot(client, product, origin, args, size, concurrency, repeat)
                    results.append(row)
                    save(args.output / "hot.json", results)
                    print(row["name"], round(row["mib_per_second"], 2), "MiB/s", flush=True)
        physical_reads(client, product, origin, args)
        save(args.output / "outcome.json", {"success": all(row["passed"] for row in checks),
             "functional_passed": sum(row["passed"] for row in checks), "functional_total": len(checks), "hot_batches": len(results)})


if __name__ == "__main__":
    main()
