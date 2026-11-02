#!/usr/bin/env python3
"""Cross-check warm-cache capacity with body-checked hey; SHA256 before/after each batch.

Reuses the acceptance origin, process lifecycle and resource sampling. Unlike the
correctness client, hey does not hash every response. Run separately from other loads.
"""
import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import signal
import subprocess
import threading

from acceptance import Client, MIB, Origin, Product, file_hash, save
from public_acceptance import counter_metrics, drain, snapshot


TEMPLATE = ('{"seconds":{{.Total.Seconds}},"requests":{{.NumRes}},"bytes":{{.SizeTotal}},'
            '"rps":{{.Rps}},"status":{{jsonify .StatusCodeDist}},'
            '"errors":{{jsonify .ErrorDist}},"latency":{{jsonify .LatencyDistribution}}}')


def checked_summary(raw, expected_bytes):
    assert raw["requests"] > 0 and raw["seconds"] > 0, raw
    assert not raw["errors"], raw
    assert raw["status"] == {"200": raw["requests"]}, raw
    # tools/build_hey.py makes SizeTotal count actual bytes and propagates body errors.
    assert raw["bytes"] == raw["requests"] * expected_bytes, raw
    return {"requests": raw["requests"], "seconds": raw["seconds"], "requests_per_second": raw["rps"],
            "mib_per_second": raw["bytes"] / MIB / raw["seconds"], "unexpected_errors": 0,
            "completion_ms": {"p" + str(row["Percentage"]): row["Latency"] * 1000
                              for row in raw["latency"] if row["Percentage"] > 0}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--hey", type=Path, required=True)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--public-static", action="store_true")
    parser.add_argument("--ram-algorithm", choices=("otter", "s3-fifo"), default="otter")
    parser.add_argument("--ram-bytes", type=int, default=10 * MIB)
    parser.add_argument("--block-bytes", type=int, choices=(MIB,), default=MIB,
                        help="compatibility option; chunk size is fixed at 1 MiB")
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    assert args.seconds > 0 and args.repeats > 0
    for key in ("binary", "hey", "fixtures", "output", "cache_root"):
        setattr(args, key, getattr(args, key).resolve())
    hey_build = json.loads(args.hey.with_suffix(".build.json").read_text())
    assert hey_build["variant"] == "razor-bodycheck" and hey_build["sha256"] == file_hash(args.hey), "use tools/build_hey.py output"
    args.output.mkdir(parents=True, exist_ok=False)
    args.cache_root.mkdir(parents=True, exist_ok=False)
    fixtures = {(size, version): args.fixtures / f"{size}-v{version}.bin"
                for size in ("small", "large") for version in (1, 2)}
    save(args.output / "environment.json", {"binary_sha256": file_hash(args.binary), "hey_sha256": file_hash(args.hey),
         "hey_variant": "v0.1.4+razor-bodycheck",
         "hey_build": hey_build,
         "binary_version": subprocess.check_output([str(args.binary), "-v"], text=True), "uname": list(os.uname()),
         "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
         "arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
         "fixture_sha256": {p.name: file_hash(p) for p in fixtures.values()},
         "measurement": "HTTP/1.1 loopback, keepalive, identity, hey cpus=8, 100ms server sampling; pre/post SHA256"})
    results = []
    with ExitStack() as cleanup:
        origin = Origin(fixtures, args.output / "origin.json")
        cleanup.callback(origin.close)
        product = Product("razor", args.output / "server", origin, args)
        cleanup.callback(product.stop)
        client = Client(fixtures, args.output)
        cleanup.callback(client.raw.close)
        for size, steps in (("small", (32, 100)), ("large", (8, 32))):
            path = f"/{size}-hey-hot"
            expected_bytes, _ = client.expected(path)
            client.checked(product, path)
            for concurrency in steps:
                for repeat in range(args.repeats):
                    name = f"{size}-c{concurrency}-r{repeat + 1}"
                    before = origin.snapshot()
                    initial = product.resource()
                    server_before = snapshot(product)
                    samples, done = [initial], threading.Event()
                    def sample():
                        while not done.wait(.1):
                            samples.append(product.resource())
                    monitor = threading.Thread(target=sample)
                    monitor.start()
                    product.proc.send_signal(signal.SIGUSR1)
                    command = [str(args.hey), "-z", f"{args.seconds}s", "-c", str(concurrency), "-t", "120", "-cpus", "8",
                               "-host", "bench.local", "-H", "Accept-Encoding: identity", "-disable-compression",
                               "-o", TEMPLATE, f"http://127.0.0.1:{product.port}{path}"]
                    try:
                        with (args.output / (name + ".json")).open("w") as output:
                            subprocess.run(command, stdout=output, stderr=subprocess.PIPE, check=True, timeout=args.seconds + 180)
                    finally:
                        done.set()
                        monitor.join()
                    samples.append(product.resource())
                    raw = json.loads((args.output / (name + ".json")).read_text())
                    result = checked_summary(raw, expected_bytes)
                    server_after = snapshot(product)
                    result.update(server_before=server_before, server_after=server_after,
                        cache_metrics=counter_metrics(server_before["stats"], server_after["stats"]),
                        process_io_bytes={key: samples[-1][key] - initial[key] for key in ("read_bytes", "write_bytes")},
                        name=name, concurrency=concurrency, object_bytes=expected_bytes,
                        rss_peak_bytes=max(row["rss_bytes"] for row in samples), fd_peak=max(row["fds"] for row in samples),
                        cpu_seconds_per_gib=(samples[-1]["cpu_seconds"] - initial["cpu_seconds"]) / (raw["bytes"] / (1 << 30)),
                        resource_samples=samples, command=command)
                    client.checked(product, path)
                    assert origin.snapshot() == before, (before, origin.snapshot())
                    results.append(result)
                    save(args.output / "batches.json", results)
                    print(name, result["requests"], round(result["mib_per_second"], 2), "MiB/s", flush=True)
        path = "/large-physical-read"
        client.checked(product, path)
        for repeat in range(args.repeats):
            product.stop()
            product.start()
            # Recovery reads full objects. Discard this private file only after it finishes.
            with product.cache_file.open("rb") as cache_file:
                os.fsync(cache_file.fileno())
                os.posix_fadvise(cache_file.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
            name = f"physical-file-cold-r{repeat + 1}"
            before = origin.snapshot()
            initial = product.resource()
            command = [str(args.hey), "-n", "1", "-c", "1", "-t", "120", "-cpus", "1", "-host", "bench.local",
                       "-H", "Accept-Encoding: identity", "-disable-compression", "-o", TEMPLATE,
                       f"http://127.0.0.1:{product.port}{path}"]
            raw = json.loads(subprocess.check_output(command, text=True, timeout=180))
            final = product.resource()
            save(args.output / (name + ".json"), raw)
            expected_bytes = fixtures[("large", 1)].stat().st_size
            row = checked_summary(raw, expected_bytes)
            row.update(name=name, process_io_bytes={k: final[k] - initial[k] for k in ("read_bytes", "write_bytes")},
                       cpu_seconds_per_gib=(final["cpu_seconds"] - initial["cpu_seconds"]) / (expected_bytes / (1 << 30)))
            # fadvise is advisory; allow residual pages but require >=99% actual disk reads.
            assert row["process_io_bytes"]["read_bytes"] >= expected_bytes * .99, row
            client.checked(product, path)
            assert origin.snapshot() == before, (before, origin.snapshot())
            results.append(row)
            save(args.output / "physical-reads.json", [r for r in results if r["name"].startswith("physical-")])
            print(name, round(row["mib_per_second"], 2), "MiB/s", row["process_io_bytes"], flush=True)
        save(args.output / "drain.json", drain(product))
    save(args.output / "outcome.json", {"success": True})


if __name__ == "__main__":
    main()
