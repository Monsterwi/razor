#!/usr/bin/env python3
"""Linux, Python stdlib only. Real binary + streamed HTTP/1.1 correctness/load client.

Each invocation owns a NEW --output directory. No existing cache is removed.
ATS is optional: --products razor runs without an ATS installation.
"""
import argparse
import concurrent.futures
from contextlib import ExitStack, suppress
import hashlib
import http.client
import http.server
import json
import math
import os
from pathlib import Path
import random
import re
import shutil
import signal
import socket
import subprocess
import threading
import time
import urllib.parse

MIB = 1 << 20
CHUNK = 64 << 10


def save(path, data):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(path)


def wait_for(check, description, timeout=15):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        if check():
            return
        time.sleep(.02)
    raise AssertionError("timeout: " + description)


def free_port():
    # Origin/health outbound allocation must not take this released listener port.
    low, high = map(int, Path("/proc/sys/net/ipv4/ip_local_port_range").read_text().split())
    candidates = [port for port in range(1024, 65536) if not low <= port <= high]
    random.shuffle(candidates)
    dual = socket.has_dualstack_ipv6()
    for port in candidates[:128]:
        with socket.socket(socket.AF_INET6 if dual else socket.AF_INET) as listener:
            if dual:
                listener.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
            try:
                listener.bind(("::" if dual else "0.0.0.0", port))
            except OSError:
                continue
            return port
    raise RuntimeError("no available nonprivileged listener port outside kernel ephemeral range")


def file_hash(path, start=0, length=None):
    h = hashlib.sha256()
    with path.open("rb") as f:
        f.seek(start)
        remaining = path.stat().st_size - start if length is None else length
        while remaining:
            b = f.read(min(CHUNK, remaining))
            assert b, "fixture unexpectedly truncated"
            h.update(b)
            remaining -= len(b)
    return h.hexdigest()


def parse_device_io(device, diskstats):
    for line in diskstats.splitlines():
        row = line.split()
        if len(row) >= 14 and (int(row[0]), int(row[1])) == (os.major(device), os.minor(device)):
            return {"major": int(row[0]), "minor": int(row[1]), "name": row[2],
                    "read_bytes": int(row[5]) * 512, "write_bytes": int(row[9]) * 512, "io_ms": int(row[12])}
    return None


def device_io(cache_file):
    try:
        return parse_device_io(cache_file.stat().st_dev, Path("/proc/diskstats").read_text())
    except OSError:
        return None


class Origin:
    def __init__(self, fixtures, output, version=1):
        self.fixtures, self.version = fixtures, version
        self.lock = threading.Lock()
        self.records, self.rules = [], {}
        self.healthy = True
        self.output = output
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def setup(self):
                super().setup()
                self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            def log_message(self, *args):
                pass

            def do_HEAD(self):
                self.respond()

            def do_GET(self):
                self.respond()

            def respond(self):
                path = urllib.parse.urlsplit(self.path).path
                with owner.lock:
                    rule = dict(owner.rules.get(path, {}))
                    row = {"path": path, "method": self.command, "bytes": 0,
                           "range": self.headers.get("Range"), "time": time.time(), "status": 0,
                           "remote_ip": self.client_address[0], "headers": dict(self.headers)}
                    owner.records.append(row)
                    if rule.get("switch_after_get") and sum(r["method"] == "GET" and r["path"] == path for r in owner.records) > rule["switch_after_get"]:
                        rule["version"] = owner.rules[path]["version"] = 2
                size = "large" if "large" in path else "small"
                if rule.get("retain_old_version") and self.headers.get("If-Match") == f'"{size}-v1"':
                    rule["version"] = 1
                fixture = owner.fixtures[(size, rule.get("version", owner.version))]
                total = fixture.stat().st_size
                tag = '"%s-v%d"' % (size, rule.get("version", owner.version))
                status, start, end = 200, 0, total - 1
                if path == "/health" and not owner.healthy:
                    status = 503
                elif self.headers.get("If-Match") not in (None, tag):
                    status = 412
                elif self.headers.get("If-None-Match") == tag:
                    status = 304
                elif self.headers.get("Range") and self.headers.get("If-Range") in (None, tag):
                    value = self.headers["Range"].removeprefix("bytes=")
                    if "," in value:
                        status = 200  # Deliberately unsupported multipart origin.
                    else:
                        left, right = value.split("-", 1)
                        if not left:
                            start = max(0, total - int(right))
                        else:
                            start, end = int(left), min(total - 1, int(right) if right else total - 1)
                        status = 206 if 0 <= start <= end < total else 416
                # Request-level rules match the selected GET start, never HEAD.
                if "block_start" in rule and (self.command != "GET" or start != rule["block_start"]):
                    rule = {}
                if rule.get("force_status"):
                    status = rule["force_status"]
                if "body_gate_at" not in rule:
                    if rule.get("entered"):
                        rule["entered"].set()
                    if rule.get("gate"):
                        rule["gate"].wait(15)
                time.sleep(rule.get("header_delay", 0))
                length = end - start + 1 if status in (200, 206) else 0
                try:
                    self.send_response(status)
                    self.send_header("Content-Length", str(length))
                    self.send_header("Cache-Control", rule.get("cache_control", "public, max-age=3600"))
                    self.send_header("Content-Type", "application/octet-stream")
                    self.send_header("ETag", tag)
                    self.send_header("Accept-Ranges", "bytes")
                    if status == 206:
                        self.send_header("Content-Range", f"bytes {start}-{end}/{total}")
                    elif status == 416:
                        self.send_header("Content-Range", f"bytes */{total}")
                    self.end_headers()
                    row["status"] = status
                    if self.command == "HEAD":
                        return
                    with fixture.open("rb") as f:
                        f.seek(start)
                        remaining = length
                        gate_at = rule.get("body_gate_at")
                        truncate_at = rule.get("truncate_at")
                        while remaining:
                            position = end + 1 - remaining
                            if position == truncate_at:
                                self.close_connection = True
                                return
                            if position == gate_at:
                                self.wfile.flush()
                                if rule.get("entered"):
                                    rule["entered"].set()
                                if rule.get("gate"):
                                    rule["gate"].wait(15)
                                gate_at = None
                            count = min(CHUNK, remaining // 2 if rule.get("truncate") else remaining)
                            if gate_at is not None and position < gate_at:
                                count = min(count, gate_at - position)
                            if truncate_at is not None and position < truncate_at:
                                count = min(count, truncate_at - position)
                            block = f.read(count)
                            self.wfile.write(block)
                            with owner.lock:
                                row["bytes"] += len(block)
                            remaining -= len(block)
                            if rule.get("truncate"):
                                self.close_connection = True
                                return
                            time.sleep(rule.get("chunk_delay", 0))
                except (BrokenPipeError, ConnectionResetError, TimeoutError):
                    row["cancelled"] = True
                finally:
                    row["end"] = time.time()

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = "http://127.0.0.1:%d" % self.server.server_port

    def snapshot(self, path=None):
        with self.lock:
            rows = [r for r in self.records if r["path"] != "/health" and (path is None or r["path"] == path)]
            return {"requests": len(rows), "get": sum(r["method"] == "GET" for r in rows),
                    "head": sum(r["method"] == "HEAD" for r in rows), "bytes": sum(r["bytes"] for r in rows)}

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        save(self.output, self.records)


class Product:
    def __init__(self, kind, directory, origin, args, capacity=1024, overrides=None):
        self.kind, self.dir, self.args = kind, directory, args
        self.dir.mkdir()
        self.port = free_port()
        self.proc = None
        self.config = {}
        self.env = dict(os.environ, GOMAXPROCS="8")
        if kind == "razor":
            self.config = {
                "http.server_ports": ("INT", self.port),
                "log.logfile_dir": ("STRING", self.dir),
                "http.parent_proxy.upstream_algo": ("STRING", "round-robin"),
                "http.parent_proxy.upstream_targets": ("STRING", origin.url),
                "http.purge_token": ("STRING", "acceptance-local-token"),
                "cache.max_object_bytes": ("INT", 256 * MIB),
                "cache.write_buffer_bytes": ("INT", 128 * MIB),
                "http.max_requests": ("INT", 256),
                "http.max_queued_requests": ("INT", 64),
                "http.transaction_no_activity_timeout_in": ("INT", 120),
                "http.transaction_no_activity_timeout_out": ("INT", 120),
                "http.shutdown_timeout": ("INT", 5),
                "http.health.path": ("STRING", "/health"),
            }
            self.config.update({
                "cache.shared_fill_public_static": ("INT", int(getattr(args, "public_static", False))),
                "cache.range_block_bytes": ("INT", getattr(args, "block_bytes", MIB)),
                "cache.ram_cache_algorithm": ("STRING", getattr(args, "ram_algorithm", "otter")),
                "cache.ram_cache_bytes": ("INT", getattr(args, "ram_bytes", 10 * MIB)),
            })
            self.config.update(overrides or {})
            self.write_config()
            cache_dir = self.dir
            if getattr(args, "cache_root", None):
                cache_dir = args.cache_root / self.dir.name
                cache_dir.mkdir(parents=True, exist_ok=False)
            self.cache_file = cache_dir / "cache.db"
            (self.dir / "storage.config").write_text(str(self.cache_file) + f" {capacity}M\n")
            self.command = [str(args.binary), "-c", str(self.dir)]
        else:
            meta = json.loads(args.ats_environment.read_text())
            template = Path(meta["runroot_example"])
            shutil.copytree(template / "etc", self.dir / "etc")
            for name in ("run", "log", "cache"):
                (self.dir / name).mkdir()
            (self.dir / "runroot.yaml").write_text((template / "runroot.yaml").read_text().replace(str(template), str(self.dir)))
            text = (self.dir / "etc/records.config").read_text().replace(str(template), str(self.dir))
            settings = {
                "http.server_ports": ("STRING", f"{self.port}:ip-in=127.0.0.1"),
                "exec_thread.limit": ("INT", 8), "cache.ram_cache.size": ("INT", 10 * MIB),
                "cache.ram_cache_cutoff": ("INT", 256 * MIB), "cache.max_doc_size": ("INT", 256 * MIB),
                "log.logging_enabled": ("INT", 3), "cache.enable_read_while_writer": ("INT", 1),
                "http.cache.open_write_fail_action": ("INT", 5), "http.cache.max_open_read_retries": ("INT", 50),
                "http.cache.open_read_retry_time": ("INT", 10),
                "cache.read_while_writer.max_retries": ("INT", 50),
                "cache.read_while_writer_retry.delay": ("INT", 10),
                "http.cache.ignore_client_no_cache": ("INT", 0),
            }
            for key, (typ, value) in settings.items():
                text = re.sub(r"(?m)^CONFIG proxy\.config\." + re.escape(key) + r" .*\n?", "", text)
                text += f"CONFIG proxy.config.{key} {typ} {value}\n"
            (self.dir / "etc/records.config").write_text(text)
            (self.dir / "etc/remap.config").write_text(f"map http://bench.local/ {origin.url}/\n")
            (self.dir / "etc/storage.config").write_text(str(self.dir / "cache") + f" {capacity}M\n")
            # Comparable per-request disk logging; ATS and zap formats remain different.
            p = self.dir / "etc/logging.yaml"
            p.write_text(p.read_text().replace("mode: binary", "mode: ascii"))
            self.env.update(LD_LIBRARY_PATH=meta["LD_LIBRARY_PATH"], TS_RUNROOT=str(self.dir))
            self.command = [meta["binary"]]
            if os.geteuid() == 0:
                for root, dirs, files in os.walk(self.dir):
                    os.chown(root, 65534, 65534)
                    for name in files:
                        os.chown(Path(root) / name, 65534, 65534)
        self.start()

    def write_config(self):
        (self.dir / "records.config").write_text("".join(f"CONFIG proxy.config.{k} {t} {v}\n" for k, (t, v) in self.config.items()))

    def start(self):
        self.log = (self.dir / "process.log").open("a")
        self.proc = None
        self.stats_offset = (self.dir / "process.log").stat().st_size
        try:
            self.proc = subprocess.Popen(self.command, stdout=self.log, stderr=self.log, env=self.env)
            def ready():
                assert self.proc.poll() is None, f"{self.kind} exited: {self.proc.returncode}; inspect {self.dir}/process.log"
                try:
                    with socket.create_connection(("127.0.0.1", self.port), timeout=.1):
                        return True
                except OSError:
                    return False
            wait_for(ready, self.kind + " listening", 30)
            time.sleep(.1)
        except BaseException:
            # A failed constructor has no caller-owned Product to stop.
            if self.proc is not None:
                with suppress(Exception):
                    self.proc.kill()
                with suppress(Exception):
                    self.proc.wait(timeout=5)
            with suppress(Exception):
                self.log.close()
            raise

    def stop(self, crash=False):
        try:
            if self.proc and self.proc.poll() is None:
                self.proc.send_signal(signal.SIGKILL if crash else signal.SIGTERM)
                try:
                    code = self.proc.wait(15)
                except subprocess.TimeoutExpired:
                    self.proc.kill()
                    self.proc.wait()
                    raise AssertionError("process failed to join on shutdown")
                if not crash:
                    assert code == 0, (self.kind, "shutdown", code)
        finally:
            self.log.close()

    def reload(self, overrides):
        assert self.kind == "razor"
        before = (self.dir / "process.log").read_text().count("Configuration reloaded:")
        self.config.update(overrides)
        self.write_config()
        self.proc.send_signal(signal.SIGHUP)
        wait_for(lambda: (self.dir / "process.log").read_text().count("Configuration reloaded:") > before, "successful reload")

    def resource(self):
        stat = Path(f"/proc/{self.proc.pid}/stat").read_text().split(") ", 1)[1].split()
        status = Path(f"/proc/{self.proc.pid}/status").read_text()
        io = dict(line.split(": ", 1) for line in Path(f"/proc/{self.proc.pid}/io").read_text().splitlines())
        return {"time": time.time(), "pid": self.proc.pid, "cpu_seconds": (int(stat[11]) + int(stat[12])) / os.sysconf("SC_CLK_TCK"),
                "read_bytes": int(io["read_bytes"]), "write_bytes": int(io["write_bytes"]),
                "rss_bytes": int(re.search(r"VmRSS:\s+(\d+)", status)[1]) * 1024,
                "fds": len(list(Path(f"/proc/{self.proc.pid}/fd").iterdir())),
                "cache_device": device_io(self.cache_file) if self.kind == "razor" else None}


class Client:
    def __init__(self, fixtures, output):
        self.fixtures, self.output = fixtures, output
        self.hashes = {}
        self.lock = threading.Lock()
        self.raw = (output / "requests.jsonl").open("a")
        self.summaries = json.loads((output / "batches.json").read_text()) if (output / "batches.json").exists() else []

    def expected(self, path, version=1, bounds=None):
        size = "large" if "large" in path else "small"
        f = self.fixtures[(size, version)]
        start, length = (0, f.stat().st_size) if bounds is None else (bounds[0], bounds[1] - bounds[0] + 1)
        key = (size, version, start, length)
        if key not in self.hashes:
            self.hashes[key] = file_hash(f, start, length)
        return length, self.hashes[key]

    def request(self, product, path, headers=None, method="GET", expected=None, status=200, slow=0, connection=None, case=None, source_ip=None):
        c = connection or http.client.HTTPConnection("127.0.0.1", product.port, timeout=120,
            source_address=(source_ip, 0) if source_ip else None)
        start = time.monotonic()
        row = {"product": product.kind, "path": path, "case": case, "time": time.time(), "method": method, "status": 0,
               "bytes": 0, "ttfb_ms": None, "error": None, "request_range": (headers or {}).get("Range")}
        digest = hashlib.sha256()
        try:
            c.request(method, path, headers={"Host": "bench.local", "Accept-Encoding": "identity", **(headers or {})})
            response = c.getresponse()
            row["status"] = response.status
            row["headers"] = dict(response.getheaders())
            first = response.read(1)
            row["ttfb_ms"] = (time.monotonic() - start) * 1000
            digest.update(first)
            row["bytes"] += len(first)
            while True:
                b = response.read(CHUNK)
                if not b:
                    break
                digest.update(b)
                row["bytes"] += len(b)
                if slow:
                    time.sleep(slow)
            row["sha256"] = digest.hexdigest()
            if status is not None and response.status != status:
                row["error"] = f"status {response.status}, expected {status}"
            if expected and response.status in (200, 206) and (row["bytes"], row["sha256"]) != expected:
                row["error"] = "length/SHA256 mismatch"
        except (OSError, http.client.HTTPException) as e:
            row["error"] = type(e).__name__ + ": " + str(e)
        finally:
            if connection is None or row["error"]:
                c.close()
        row["completion_ms"] = (time.monotonic() - start) * 1000
        with self.lock:
            self.raw.write(json.dumps(row) + "\n")
            self.raw.flush()
        return row

    def checked(self, product, path, **kwargs):
        if "expected" not in kwargs:
            kwargs["expected"] = self.expected(path)
        result = self.request(product, path, **kwargs)
        assert not result["error"], result
        return result

    def batch(self, product, origin, name, path, concurrency, count=None, duration=None, headers=None, bounds=None, slow=0, allowed=(), ranges=None, paths=None, unique_paths=False):
        expected = self.expected(path, bounds=bounds)
        variants = [(headers, expected)] if ranges is None else [
            ({**(headers or {}), "Range": "bytes=%d-%d" % limits}, self.expected(path, bounds=limits))
            for limits in ranges]
        path_expected = {p: self.expected(p) for p in paths or ()}
        before = origin.snapshot()
        structured_before = None
        if product.kind == "razor":
            from public_acceptance import snapshot
            structured_before = snapshot(product)
        initial = product.resource()
        logs_before = (product.dir / "process.log").stat().st_size
        if product.kind == "razor":
            product.proc.send_signal(signal.SIGUSR1)
            def has_snapshot():
                with (product.dir / "process.log").open() as f:
                    f.seek(logs_before)
                    return "Goroutines:" in f.read()
            wait_for(has_snapshot, "resource snapshot")
        samples, done = [initial], threading.Event()
        def sample():
            next_signal = 0
            while not done.wait(.1):
                samples.append(product.resource())
                if product.kind == "razor" and time.monotonic() >= next_signal:
                    product.proc.send_signal(signal.SIGUSR1)
                    next_signal = time.monotonic() + .5
        monitor = threading.Thread(target=sample)
        monitor.start()
        barrier = threading.Barrier(concurrency)
        start = time.monotonic()
        harness_cpu = time.process_time()
        def worker(index):
            request_headers, request_expected = variants[index % len(variants)]
            request_paths = list(paths or [path])
            random.Random(index).shuffle(request_paths)
            c = http.client.HTTPConnection("127.0.0.1", product.port, timeout=120)
            rows = []
            barrier.wait()
            try:
                while (duration is not None and time.monotonic() - start < duration) or (count is not None and len(rows) < count):
                    request_path = request_paths[len(rows) % len(request_paths)]
                    if unique_paths:
                        request_path += f"-{index}-{len(rows)}"
                    if ranges is not None:
                        request_headers, request_expected = variants[(index + len(rows) * concurrency) % len(variants)]
                    rows.append(self.request(product, request_path, headers=request_headers,
                                             expected=path_expected.get(request_path, request_expected),
                                             status=206 if bounds or ranges else 200, slow=slow, connection=c, case=name))
            finally:
                c.close()
            return rows
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
                rows = [r for group in pool.map(worker, range(concurrency)) for r in group]
        finally:
            done.set()
            monitor.join()
        elapsed = time.monotonic() - start
        samples.append(product.resource())
        after = origin.snapshot()
        payload = sum(r["bytes"] for r in rows if r["status"] in (200, 206) and not r["error"])
        errors = [r for r in rows if r["error"] and r["status"] not in allowed]
        with (product.dir / "process.log").open() as f:
            f.seek(logs_before)
            logs = f.read()
        goroutines = [int(n) for n in re.findall(r"Goroutines:(\d+)", logs)]
        def quantiles(key):
            values = sorted(r[key] for r in rows if r[key] is not None)
            return {"p" + str(q): values[max(0, math.ceil(len(values) * q / 100) - 1)] for q in (50, 95, 99)}
        summary = {"name": name, "product": product.kind, "concurrency": concurrency, "requests": len(rows),
                   "seconds": elapsed, "requests_per_second": len(rows) / elapsed,
                   "mib_per_second": payload / MIB / elapsed, "ttfb_ms": quantiles("ttfb_ms"),
                   "completion_ms": quantiles("completion_ms"), "unexpected_errors": len(errors),
                   "object_requests": {size: sum(size in r["path"] for r in rows) for size in ("small", "large")},
                   "harness_cpu_cores": (time.process_time() - harness_cpu) / elapsed,
                   "process_io_bytes": {k: samples[-1][k] - initial[k] for k in ("read_bytes", "write_bytes")},
                   "statuses": {str(s): sum(r["status"] == s for r in rows) for s in set(r["status"] for r in rows)},
                   "origin": {k: after[k] - before[k] for k in after},
                   "cpu_seconds_per_gib": (samples[-1]["cpu_seconds"] - initial["cpu_seconds"]) / (payload / (1 << 30)) if payload else None,
                   "rss_peak_bytes": max(s["rss_bytes"] for s in samples), "fd_peak": max(s["fds"] for s in samples),
                   "goroutine_peak_sampled": max(goroutines) if goroutines else None,
                   "server_stats_peak_sampled": {field: max(map(int, re.findall(r"\b" + field + r":(\d+)", logs)), default=0)
                                                  for field in ("InFlight", "CacheWriters", "BufferReserved", "SharedFillTasks", "SharedFillWaiters")}
                                                  if product.kind == "razor" else None,
                   "resource_samples": samples}
        if product.kind == "razor":
            from public_acceptance import counter_metrics, snapshot
            structured_after = snapshot(product)
            summary["server_before"] = structured_before
            summary["server_after"] = structured_after
            summary["cache_metrics"] = counter_metrics(structured_before["stats"], structured_after["stats"])
            actual_origin = summary["origin"]["bytes"]
            summary["write_amplification_per_origin_body_byte"] = summary["process_io_bytes"]["write_bytes"] / actual_origin if actual_origin else None
            first_device, last_device = initial.get("cache_device"), samples[-1].get("cache_device")
            summary["cache_device_io_delta"] = ({key: last_device[key] - first_device[key]
                for key in ("read_bytes", "write_bytes", "io_ms")} if first_device and last_device and
                (first_device["major"], first_device["minor"]) == (last_device["major"], last_device["minor"]) else None)
            summary["server_stats_samples"] = [json.loads(line.split("HTTP resource stats JSON: ", 1)[1])
                for line in logs.splitlines() if "HTTP resource stats JSON: " in line]
        self.summaries.append(summary)
        save(self.output / "batches.json", self.summaries)
        print(json.dumps({k: v for k, v in summary.items() if k not in ("resource_samples", "server_stats_samples", "server_before", "server_after")}), flush=True)
        assert not errors, errors[:2]
        return summary


def functional(args, fixtures, output):
    with ExitStack() as cleanup:
        origin = Origin(fixtures, output / "origin-functional.json")
        cleanup.callback(origin.close)
        alternate = Origin(fixtures, output / "origin-alternate.json", version=2)
        cleanup.callback(alternate.close)
        client = Client(fixtures, output)
        cleanup.callback(client.raw.close)
        product = Product("razor", output / "functional", origin, args)
        cleanup.callback(product.stop)
        large_bytes = fixtures[("large", 1)].stat().st_size
        large_label = f"{large_bytes // MIB}MiB"
        evidence = []
        def record(name, **details):
            evidence.append({"name": name, "result": "PASS", **details})
            save(output / "functional.json", evidence)
            print("PASS " + name, flush=True)
        path = "/small-cache"
        for _ in range(2):
            client.checked(product, path)
        assert origin.snapshot(path)["get"] == 1
        client.checked(product, path, headers={"Cache-Control": "no-cache"})
        assert origin.snapshot(path)["get"] == 2
        client.checked(product, path, headers={"If-None-Match": '"small-v1"'}, status=304, expected=None)
        assert origin.snapshot(path)["get"] == 2
        record("small MISS/HIT/REVALIDATE/client304", origin=origin.snapshot(path))
        for path, headers in [("/small-private", {}), ("/small-auth", {"Authorization": "Bearer test-only"})]:
            origin.rules[path] = {"cache_control": "private, max-age=3600"}
            for _ in range(2):
                client.checked(product, path, headers=headers)
            assert origin.snapshot(path)["get"] == 2
        record("private and Authorization bypass")
        for _ in range(2):
            client.checked(product, "/large-full")
        assert origin.snapshot("/large-full")["get"] == 1
        record(large_label + " full MISS/HIT", origin=origin.snapshot("/large-full"), sha256=client.expected("/large-full")[1])
        for size, concurrency in [("small", 100), ("large", 16)]:
            path = f"/{size}-coalesced"
            origin.rules[path] = {"header_delay": .5}
            summary = client.batch(product, origin, "functional-coalescing-" + size, path, concurrency, count=1)
            assert summary["origin"]["get"] == 1, summary
            record(size + " concurrent shared fill", requests=concurrency, origin=summary["origin"])
        path = "/large-ranges"
        for bounds in [(0, 1572863), (1048576, 2097151)]:
            client.checked(product, path, headers={"Range": "bytes=%d-%d" % bounds}, expected=client.expected(path, bounds=bounds), status=206)
        assert origin.snapshot(path)["bytes"] == 2 * MIB
        record("overlap 2.5MiB responses fetch only2MiB", origin=origin.snapshot(path))
        client.checked(product, path, headers={"Range": "bytes=0-"}, status=206)
        assert origin.snapshot(path)["bytes"] == large_bytes
        for value, bounds in [("bytes=-17", (large_bytes - 17, large_bytes - 1)),
                              (f"bytes={large_bytes - 10 * MIB}-", (large_bytes - 10 * MIB, large_bytes - 1))]:
            client.checked(product, path, headers={"Range": value}, expected=client.expected(path, bounds=bounds), status=206)
        assert origin.snapshot(path)["bytes"] == large_bytes
        client.checked(product, path, headers={"Range": f"bytes={large_bytes}-"}, expected=None, status=416)
        client.checked(product, path, headers={"Range": "bytes=0-16", "If-Range": '"old"'})
        record(large_label + " native Range / suffix / open /416 / If-Range mismatch")
        overlap = [(i * 8 * MIB, (i * 8 + 16) * MIB - 1) for i in range(4)]
        for concurrency in (8, 32):
            summary = client.batch(product, origin, f"different-overlapping-ranges-c{concurrency}",
                                   "/large-overlap-stress", concurrency, count=1, ranges=overlap)
            assert summary["origin"]["bytes"] == (40 * MIB if concurrency == 8 else 0), summary
            assert summary["origin"]["get"] == (20 if concurrency == 8 else 0), summary
        actual_ranges = [row["range"] for row in origin.records
                         if row["path"] == "/large-overlap-stress" and row["method"] == "GET"]
        expected_ranges = [f"bytes={start}-{start + 2 * MIB - 1}" for start in range(0, 40 * MIB, 2 * MIB)]
        assert sorted(actual_ranges) == sorted(expected_ranges), actual_ranges
        record("8/32 clients with four overlapping16MiB ranges:40MiB union fetched once")
        changed = "/large-version-swap"
        origin.rules[changed] = {"switch_after_get": 1}
        bounds = (0, 3 * MIB - 1)
        result = client.request(product, changed, headers={"Range": f"bytes=0-{bounds[1]}"}, expected=client.expected(changed, bounds=bounds), status=206)
        assert result["error"], result
        client.checked(product, changed, headers={"Range": f"bytes=0-{bounds[1]}"}, expected=client.expected(changed, version=2, bounds=bounds), status=206)
        record("Range version changes mid-fill: controlled failure then exact new version")
        before = origin.snapshot()
        product.stop()
        product.start()
        client.checked(product, "/large-full")
        client.checked(product, path, headers={"Range": "bytes=-17"}, expected=client.expected(path, bounds=(large_bytes - 17, large_bytes - 1)), status=206)
        assert origin.snapshot() == before, (before, origin.snapshot())
        record("clean restart reuses full object and verified blocks")
        for path in ("/large-full", "/large-ranges"):
            client.checked(product, path, method="PURGE", headers={"Authorization": "Bearer acceptance-local-token"}, expected=None)
        before = origin.snapshot()
        client.checked(product, "/large-full")
        client.checked(product, "/large-ranges", headers={"Range": "bytes=0-16"}, expected=client.expected("/large-ranges", bounds=(0, 16)), status=206)
        assert origin.snapshot()["bytes"] - before["bytes"] == large_bytes + MIB
        record("authenticated exact PURGE fences full object and range family")
        # Creator closes its TCP request before headers; follower still gets the full object.
        path = "/large-cancel"
        gate, entered = threading.Event(), threading.Event()
        origin.rules[path] = {"gate": gate, "entered": entered}
        first = socket.create_connection(("127.0.0.1", product.port))
        first.sendall((f"GET {path} HTTP/1.1\r\nHost: bench.local\r\nAccept-Encoding: identity\r\n\r\n").encode())
        assert entered.wait(5)
        with concurrent.futures.ThreadPoolExecutor() as pool:
            follower = pool.submit(client.checked, product, path)
            time.sleep(.2)
            first.shutdown(socket.SHUT_RDWR)
            first.close()
            gate.set()
            follower.result()
        assert origin.snapshot(path)["get"] == 1
        record("creator cancellation leaves follower's" + large_label + " fill alive")
        # Eligible fills purged before response headers fail without a version retry.
        path = "/small-purge-inflight"
        gate, entered = threading.Event(), threading.Event()
        origin.rules[path] = {"gate": gate, "entered": entered}
        with concurrent.futures.ThreadPoolExecutor() as pool:
            old = pool.submit(client.request, product, path, expected=client.expected(path), status=None)
            assert entered.wait(5)
            follower = pool.submit(client.request, product, path, expected=client.expected(path))
            before_log = (product.dir / "process.log").stat().st_size
            def subscribed():
                product.proc.send_signal(signal.SIGUSR1)
                with (product.dir / "process.log").open() as f:
                    f.seek(before_log)
                    return "SharedFillWaiters:2" in f.read()
            wait_for(subscribed, "creator and follower subscribed")
            client.checked(product, path, method="PURGE", headers={"Authorization": "Bearer acceptance-local-token"}, expected=None)
            origin.rules[path] = {"version": 2}
            client.checked(product, path, expected=client.expected(path, version=2))
            gate.set()
            for result in (old.result(), follower.result()):
                assert result["status"] >= 500, result
        client.checked(product, path, expected=client.expected(path, version=2))
        assert origin.snapshot(path)["get"] == 2
        record("pre-header PURGE: old subscribers fail without retry; new version stays reusable", origin=origin.snapshot(path))
        path = "/large-truncated"
        origin.rules[path] = {"truncate": True}
        result = client.request(product, path, expected=client.expected(path))
        assert result["error"] and result["bytes"] < large_bytes, result
        origin.rules[path] = {}
        client.checked(product, path)
        assert origin.snapshot(path)["get"] == 2
        record("truncated origin never commits a successful partial object")
        path = "/large-crash"
        entered = threading.Event()
        origin.rules[path] = {"chunk_delay": .003, "entered": entered}
        with concurrent.futures.ThreadPoolExecutor() as pool:
            interrupted = pool.submit(client.request, product, path, expected=client.expected(path))
            assert entered.wait(5)
            time.sleep(.2)
            product.stop(crash=True)
            result = interrupted.result()
            assert result["error"], result
        origin.rules[path] = {}
        product.start()
        client.checked(product, path)
        assert origin.snapshot(path)["get"] == 2
        record("SIGKILL during" + large_label + " fill; restart refetches exact complete body")
        path = "/large-reload"
        entered, gate = threading.Event(), threading.Event()
        origin.rules[path] = {"entered": entered, "gate": gate}
        with concurrent.futures.ThreadPoolExecutor() as pool:
            old = pool.submit(client.checked, product, path)
            assert entered.wait(5)
            product.reload({"http.parent_proxy.upstream_targets": ("STRING", alternate.url)})
            client.checked(product, "/small-new-runtime", expected=client.expected("/small-new-runtime", version=2))
            gate.set()
            old.result()
        client.checked(product, path, expected=client.expected(path, version=2))
        record("SIGHUP pins old" + large_label + " response; new runtime uses new origin")
        product.reload({"http.parent_proxy.upstream_targets": ("STRING", origin.url),
                        "http.health.interval": ("INT", 1), "http.health.failures": ("INT", 1),
                        "http.health.successes": ("INT", 1)})
        client.checked(product, "/small-health-full")
        client.checked(product, "/small-health-range", headers={"Range": "bytes=0-16"}, expected=client.expected("/small-health-range", bounds=(0, 16)), status=206)
        origin.healthy = False
        probe_headers = {"Authorization": "Bearer health-probe-only"}
        wait_for(lambda: client.request(product, "/small-health-probe", headers=probe_headers, status=None)["status"] == 502,
                 "all origins removed by health checks")
        client.checked(product, "/small-health-full")
        before = origin.snapshot("/small-health-range")
        client.checked(product, "/small-health-range", headers={"Range": "bytes=0-16"}, expected=client.expected("/small-health-range", bounds=(0, 16)), status=206)
        after = origin.snapshot("/small-health-range")
        assert after == before, (before, after)
        record("fresh Range with all origins unhealthy", status=206, origin_delta={key: after[key] - before[key] for key in before})
        origin.healthy = True
        wait_for(lambda: client.request(product, "/small-health-probe", headers=probe_headers, status=None)["status"] == 200,
                 "healthy origin rejoined")
        client.checked(product, "/small-health-probe", headers=probe_headers)
        client.checked(product, "/small-health-range", headers={"Range": "bytes=0-16"}, expected=client.expected("/small-health-range", bounds=(0, 16)), status=206)
        product.reload({"http.health.interval": ("INT", 0), "http.max_requests": ("INT", 2), "http.max_queued_requests": ("INT", 0)})
        origin.rules["/small-overload"] = {"header_delay": .5}
        summary = client.batch(product, origin, "bounded-request-overload", "/small-overload", 32, count=1, allowed=(503,))
        assert int(summary["statuses"].get("503", 0)) > 0 and int(summary["statuses"].get("200", 0)) > 0
        record("saturation rejects503 and recovers", statuses=summary["statuses"])
        client.checked(product, "/small-after-overload")
        product.stop()
        product = Product("razor", output / "capacity", origin, args, capacity=32)
        cleanup.callback(product.stop)
        for i in range(160):
            client.checked(product, f"/small-wrap-{i}")
        client.checked(product, "/small-wrap-0")
        assert origin.snapshot("/small-wrap-0")["get"] == 2
        before = origin.snapshot("/large-over-capacity")
        client.checked(product, "/large-over-capacity", headers={"Range": "bytes=0-"}, status=206)
        assert origin.snapshot("/large-over-capacity")["bytes"] - before["bytes"] == large_bytes
        record("bounded32MiB disk wraps safely; larger Range streams through bounded window")
        product.stop()
        # Verify advertised main-binary cache states actually appeared in access logs.
        access = (output / "functional/access.log").read_text()
        for state in ("MISS", "HIT", "REVALIDATE", "BYPASS", "PURGE"):
            assert f'"cache_status":"{state}"' in access, state
        record("main access-log MISS/HIT/REVALIDATE/BYPASS/PURGE observed")

def benchmark(args, fixtures, output):
    with ExitStack() as cleanup:
        origin = Origin(fixtures, output / "origin-benchmark.json")
        cleanup.callback(origin.close)
        client = Client(fixtures, output)
        cleanup.callback(client.raw.close)
        for kind in args.products:
            product = Product(kind, output / (kind + "-benchmark"), origin, args)
            try:
                for size, steps in [("small", (1, 8, 32, 100)), ("large", (1, 8, 32))]:
                    path = f"/{size}-hot-{kind}"
                    client.checked(product, path)
                    for c in steps:
                        for repeat in range(args.repeats):
                            summary = client.batch(product, origin, f"{size}-hot-c{c}-r{repeat + 1}", path, c, count=1, duration=args.seconds)
                            assert summary["origin"]["get"] == 0, summary
                    for c in (1, 8, 32):
                        path = f"/{size}-cold-{kind}-{c}"
                        # A real slow origin lets all cold clients contend before data arrives.
                        origin.rules[path] = {"header_delay": .2}
                        summary = client.batch(product, origin, f"{size}-cold-c{c}", path, c, count=1)
                        if kind == "razor":
                            assert summary["origin"]["get"] == 1, summary
                path = f"/large-slow-origin-{kind}"
                origin.rules[path] = {"chunk_delay": .001}
                client.batch(product, origin, "large-slow-origin-c8", path, 8, count=1)
                client.batch(product, origin, "large-slow-client-c8", path, 8, count=1, slow=.001)
                if kind == "razor":
                    paths = ["/small-mixed-hot"] * 9 + [f"/small-mixed-{i}" for i in range(7)] + ["/large-mixed-0", "/large-mixed-1"] * 2
                    for path in dict.fromkeys(paths):
                        client.checked(product, path)
                    for c in (8, 32):
                        for repeat in range(args.repeats):
                            summary = client.batch(product, origin, f"mixed-hot-c{c}-r{repeat + 1}", paths[0], c,
                                                   count=1, duration=args.seconds, paths=paths)
                            assert summary["origin"]["get"] == 0, summary
                    summary = client.batch(product, origin, "small-longtail-unique-c8", "/small-unique", 8, count=8, unique_paths=True)
                    assert summary["origin"]["get"] == 64, summary
                    assert summary["origin"]["bytes"] == 64 * fixtures[("small", 1)].stat().st_size, summary
                    path = "/large-range-overlap-load"
                    for c in (1, 8, 32):
                        bounds = (0, 16 * MIB - 1)
                        client.batch(product, origin, f"range16MiB-c{c}", path, c, count=1, duration=args.seconds,
                                     headers={"Range": "bytes=0-%d" % bounds[1]}, bounds=bounds)
                    path = "/large-disk-read-probe"
                    client.checked(product, path)
                    for repeat in range(args.repeats):
                        product.stop()
                        product.start()
                        # Recovery validates full bodies, so evict afterwards, while idle.
                        # Only our cache file: no global drop_caches or other workloads.
                        with product.cache_file.open("rb") as cache_file:
                            os.fsync(cache_file.fileno())
                            os.posix_fadvise(cache_file.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
                        summary = client.batch(product, origin, f"large-file-cache-cold-r{repeat + 1}", path, 1, count=1)
                        assert summary["origin"]["get"] == 0, summary
                # A quiet post-load sample makes drained tasks/leases observable.
                time.sleep(1)
                if kind == "razor":
                    product.proc.send_signal(signal.SIGUSR1)
                    time.sleep(.1)
            finally:
                product.stop()

def create_fixtures(fixture_dir, large_bytes):
    fixtures = {}
    fixture_dir.mkdir()
    for size, length in (("small", 256 << 10), ("large", large_bytes)):
        for version in (1, 2):
            path = fixture_dir / f"{size}-v{version}.bin"
            source = random.Random(100 + version)
            with path.open("wb") as f:
                for offset in range(0, length, CHUNK):
                    f.write(source.randbytes(min(CHUNK, length - offset)))
            fixtures[(size, version)] = path
    return fixtures


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--ats-environment", type=Path)
    p.add_argument("--products", nargs="+", choices=("razor", "ats"), default=["razor", "ats"])
    p.add_argument("--large-mib", type=int, choices=(100, 200), default=100)
    p.add_argument("--public-static", action="store_true", help="explicit public identity contract; required for public-functional and soak")
    p.add_argument("--ram-algorithm", choices=("otter", "s3-fifo"), default="otter")
    p.add_argument("--ram-bytes", type=int, default=10 * MIB, help="same per-stripe payload budget for policy comparison")
    p.add_argument("--block-bytes", type=int, choices=(MIB,), default=MIB,
                   help="compatibility option; chunk size is fixed at 1 MiB")
    p.add_argument("--soak-seconds", type=float, default=7200)
    p.add_argument("--allow-short-soak", action="store_true", help="smoke test only; never qualifies as two-hour acceptance")
    p.add_argument("--cache-root", type=Path, help="new Razor cache directory on the disk under test; results stay under --output")
    p.add_argument("--mode", choices=("functional", "public-functional", "benchmark", "soak", "all"), default="all")
    p.add_argument("--seconds", type=float, default=4)
    p.add_argument("--repeats", type=int, default=3)
    args = p.parse_args()
    assert args.seconds > 0 and args.repeats > 0
    args.binary, args.output = args.binary.resolve(), args.output.resolve()
    if args.public_static:
        assert args.products == ["razor"], "--public-static requires --products razor"
    if args.mode in ("public-functional", "soak"):
        assert args.public_static, "public modes require --public-static"
    if args.mode in ("benchmark", "all") and "ats" in args.products:
        assert args.ats_environment, "ATS environment JSON is required for the comparison"
    args.output.mkdir(parents=True, exist_ok=False)
    if args.cache_root:
        args.cache_root = args.cache_root.resolve()
        args.cache_root.mkdir(parents=True, exist_ok=False)
    fixtures = create_fixtures(args.output / "fixtures", args.large_mib * MIB)
    metadata = {"time_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                "source_status": subprocess.check_output(["git", "status", "--porcelain"], text=True),
                "binary": str(args.binary), "binary_sha256": file_hash(args.binary),
                "binary_version": subprocess.check_output([str(args.binary), "-v"], text=True).strip(),
                "uname": list(os.uname()), "cpu_count": os.cpu_count(), "cpuinfo": Path("/proc/cpuinfo").read_text(),
                "meminfo": Path("/proc/meminfo").read_text(), "disk": list(shutil.disk_usage(args.output)),
                "go_build": subprocess.check_output(["go", "version", "-m", str(args.binary)], text=True,
                    env=dict(os.environ, GOTOOLCHAIN="local")).strip(),
                "python": subprocess.check_output(["python3", "--version"], text=True).strip(),
                "arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                "fixture_sha256": {str(path.name): file_hash(path) for path in fixtures.values()},
                "measurement": "HTTP/1.1 loopback, 64KiB streaming, SHA256 every body, /proc 100ms, SIGUSR1 500ms; no OS cache flush"}
    save(args.output / "environment.json", metadata)
    try:
        if args.mode in ("functional", "all"):
            from copy import copy
            conservative = copy(args)
            conservative.public_static = False
            functional(conservative, fixtures, args.output)
        if args.mode == "public-functional" or (args.mode == "all" and args.public_static):
            from public_acceptance import public_functional
            public_functional(args, fixtures, args.output)
        if args.mode == "soak":
            from public_acceptance import mixed_soak
            mixed_soak(args, fixtures, args.output)
        if args.mode in ("benchmark", "all"):
            if args.public_static:
                from public_acceptance import public_benchmark
                public_benchmark(args, fixtures, args.output)
            else:
                benchmark(args, fixtures, args.output)
    except BaseException as e:
        save(args.output / "outcome.json", {"success": False, "error": repr(e)})
        raise
    save(args.output / "outcome.json", {"success": True})


if __name__ == "__main__":
    main()
