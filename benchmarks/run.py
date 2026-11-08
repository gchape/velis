"""Measure core operations and localhost TCP workloads with repeatable inputs.

Run with ``uv run python benchmarks/run.py --output benchmarks/results.json``.
Setup, prefill and teardown are excluded from timed regions. Full garbage
collection runs before each trial and remains enabled during measurement. Each
case gets one unreported warm-up and five measured trials by default. TCP clients
and server share one asyncio event loop in this process.
"""

import argparse
import asyncio
import gc
import hashlib
import json
import math
import os
import platform
import random
import statistics
import time
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any

from velis.app import serve
from velis.codec import Deflate
from velis.config import Config, Server
from velis.core.broker import Broker
from velis.core.commands import Set
from velis.core.engine import Engine
from velis.core.store import EvictionPolicy, Store, StoreLimits
from velis.core.timing_wheel import TimingWheel
from velis.net.client import Client
from velis.net.protocol import DEFAULT_PROTOCOL

_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Trial:
    """One timed run, with individual request latencies when applicable."""

    seconds: float
    operations: int
    latencies_us: list[float] = field(default_factory=list)
    deliveries: int = 0


@dataclass(frozen=True)
class Case:
    """A named workload and its setup-inclusive trial factory."""

    name: str
    parameters: dict[str, Any]
    run: Callable[[], Awaitable[Trial]]


def elapsed(started: int) -> float:
    """Convert elapsed perf-counter nanoseconds to seconds."""
    return (time.perf_counter_ns() - started) / 1e9


def percentile(values: list[float], fraction: float) -> float:
    """Return the nearest-rank percentile from a nonempty sorted sample."""
    return values[max(0, math.ceil(fraction * len(values)) - 1)]


def summarize(trials: list[Trial]) -> dict[str, Any]:
    """Summarize rates and use medians of per-trial latency percentiles."""
    rows = []
    for trial in trials:
        row = {
            "seconds": trial.seconds,
            "operations": trial.operations,
            "operations_per_second": trial.operations / trial.seconds,
        }
        if trial.latencies_us:
            ordered = sorted(trial.latencies_us)
            row.update({f"p{p}_us": percentile(ordered, p / 100) for p in (50, 95, 99)})
        if trial.deliveries:
            row["deliveries_per_second"] = trial.deliveries / trial.seconds
        rows.append(row)
    rates = [row["operations_per_second"] for row in rows]
    result: dict[str, Any] = {
        "median_operations_per_second": statistics.median(rates),
        "min_operations_per_second": min(rates),
        "max_operations_per_second": max(rates),
        "median_ns_per_operation": statistics.median(
            trial.seconds * 1e9 / trial.operations for trial in trials
        ),
        "trials": rows,
    }
    for name in ("p50_us", "p95_us", "p99_us", "deliveries_per_second"):
        if name in rows[0]:
            result[f"median_{name}"] = statistics.median(row[name] for row in rows)
    return result


async def store_case(policy: EvictionPolicy, count: int, evict: bool) -> Trial:
    """Measure 256-byte hits or new-key writes against a full 4096-key store."""
    capacity = 4096
    value = b"v" * 256
    store = Store(limits=StoreLimits(max_keys=capacity, eviction_policy=policy))
    resident = [f"key:{i}" for i in range(capacity)]
    for key in resident:
        store.set(key, value)
    keys = [f"new:{i}" for i in range(count)] if evict else resident
    started = time.perf_counter_ns()
    if evict:
        for key in keys:
            store.set(key, value)
    else:
        for i in range(count):
            if store.get(keys[i % capacity]) is not value:
                raise AssertionError("unexpected cache miss")
    seconds = elapsed(started)
    assert len(store) == capacity
    return Trial(seconds, count)


async def wheel_case(count: int, operation: str) -> Trial:
    """Measure scheduling, replacement, cancellation or bounded due draining."""
    wheel = TimingWheel(0)
    keys = [f"timer:{i}" for i in range(count)]
    if operation != "schedule":
        for i, key in enumerate(keys):
            wheel.set(key, 1 + i % 4096)
    started = time.perf_counter_ns()
    match operation:
        case "schedule" | "reschedule":
            base = 4097 if operation == "reschedule" else 1
            for i, key in enumerate(keys):
                wheel.set(key, base + i % 4096)
        case "cancel":
            for key in keys:
                assert wheel.clear(key)
        case "expire":
            expired = 0
            while wheel.has_due(4096):
                expired += len(wheel.pop_due(4096, 1000))
            assert expired == count
        case _:
            raise ValueError(operation)
    seconds = elapsed(started)
    assert len(wheel) == (count if operation in ("schedule", "reschedule") else 0)
    return Trial(seconds, count)


async def codec_case(count: int, compression: str | None = None) -> Trial:
    """Measure complete wire round-trips or 1 KiB DEFLATE round-trips."""
    if compression is None:
        codec = DEFAULT_PROTOCOL.commands
        command = Set("bench:key", b"v" * 256)
        started = time.perf_counter_ns()
        for _ in range(count):
            decoded = codec.decode(codec.encode(command))
        seconds = elapsed(started)
        assert decoded == command
    else:
        deflate = Deflate()
        value = (
            (b"velis:" * 171)[:1024]
            if compression == "repeated"
            else random.Random(0).randbytes(1024)
        )
        started = time.perf_counter_ns()
        for _ in range(count):
            restored = deflate.decode(deflate.encode(value))
        seconds = elapsed(started)
        assert restored == value
    return Trial(seconds, count)


async def tcp_get_case(
    count: int, payload_bytes: int, depth: int, policy: EvictionPolicy
) -> Trial:
    """Measure verified GETs and individual latencies after coroutine entry."""
    value = b"v" * payload_bytes
    store = Store(limits=StoreLimits(eviction_policy=policy))
    keys = [f"key:{i}" for i in range(1024)]
    for key in keys:
        store.set(key, value)
    config = Config(server=Server("127.0.0.1", 0))
    latencies: list[float] = []
    async with (
        serve(config, engine=Engine(store, Broker())) as server,
        await Client.connect("127.0.0.1", server.port) as client,
    ):

        async def get(key: str) -> None:
            started = time.perf_counter_ns()
            result = await client.get(key)
            latencies.append((time.perf_counter_ns() - started) / 1000)
            assert result == value

        started = time.perf_counter_ns()
        if depth == 1:
            for i in range(count):
                await get(keys[i % len(keys)])
        else:
            for start in range(0, count, depth):
                await asyncio.gather(
                    *(
                        get(keys[i % len(keys)])
                        for i in range(start, min(start + depth, count))
                    )
                )
        seconds = elapsed(started)
    assert len(latencies) == count
    return Trial(seconds, count, latencies)


async def pubsub_case(count: int, payload_bytes: int, fanout: int) -> Trial:
    """Time acknowledged publications through completion of every delivery."""
    value = b"v" * payload_bytes
    config = Config(server=Server("127.0.0.1", 0))
    latencies: list[float] = []
    async with AsyncExitStack() as resources:
        server = await resources.enter_async_context(serve(config))
        publisher = await resources.enter_async_context(
            await Client.connect("127.0.0.1", server.port)
        )
        subscribers = [
            await resources.enter_async_context(
                await Client.connect("127.0.0.1", server.port)
            )
            for _ in range(fanout)
        ]
        for subscriber in subscribers:
            await subscriber.subscribe("bench")

        async def consume(subscriber: Client) -> None:
            for _ in range(count):
                message = await subscriber.next_message()
                assert message.channel == "bench" and message.payload == value

        started = time.perf_counter_ns()
        async with asyncio.TaskGroup() as consumers:
            for subscriber in subscribers:
                consumers.create_task(consume(subscriber))
            for _ in range(count):
                sent = time.perf_counter_ns()
                delivered = await publisher.publish("bench", value)
                latencies.append((time.perf_counter_ns() - sent) / 1000)
                assert delivered == fanout
        seconds = elapsed(started)
    return Trial(seconds, count, latencies, count * fanout)


def cases(args: argparse.Namespace) -> list[Case]:
    """Describe the measured inputs so the JSON contains each workload's scope."""
    result = []
    for policy in (EvictionPolicy.LRU, EvictionPolicy.SIEVE):
        for evict in (False, True):
            name = f"store_{policy}_{'evict_set' if evict else 'get_hit'}"
            result.append(
                Case(
                    name,
                    {"payload_bytes": 256, "capacity": 4096},
                    partial(store_case, policy, args.core_operations, evict),
                )
            )
    for operation in ("schedule", "reschedule", "cancel", "expire"):
        result.append(
            Case(
                f"wheel_{operation}",
                {
                    "timers": args.core_operations,
                    "sweep_budget": 1000,
                    "deadline_range_ms": [4097, 8192]
                    if operation == "reschedule"
                    else [1, 4096],
                },
                partial(wheel_case, args.core_operations, operation),
            )
        )
    result.append(
        Case(
            "wire_roundtrip",
            {"payload_bytes": 256},
            partial(codec_case, args.core_operations),
        )
    )
    for compression in ("repeated", "random"):
        result.append(
            Case(
                f"deflate_{compression}_roundtrip",
                {"payload_bytes": 1024},
                partial(codec_case, max(1, args.core_operations // 10), compression),
            )
        )
    for payload in (64, 1024):
        for depth in (1, 32, 128):
            result.append(
                Case(
                    f"tcp_get_lru_{payload}b_depth{depth}",
                    {
                        "payload_bytes": payload,
                        "pipeline_depth": depth,
                        "resident_keys": 1024,
                    },
                    partial(
                        tcp_get_case,
                        args.network_operations,
                        payload,
                        depth,
                        EvictionPolicy.LRU,
                    ),
                )
            )
    result.append(
        Case(
            "tcp_get_sieve_1024b_depth32",
            {"payload_bytes": 1024, "pipeline_depth": 32, "resident_keys": 1024},
            partial(
                tcp_get_case, args.network_operations, 1024, 32, EvictionPolicy.SIEVE
            ),
        )
    )
    for payload in (64, 1024):
        for fanout in (1, 8):
            result.append(
                Case(
                    f"pubsub_{payload}b_fanout{fanout}",
                    {"payload_bytes": payload, "subscribers": fanout},
                    partial(pubsub_case, args.publications, payload, fanout),
                )
            )
    return result


def environment(root: Path) -> dict[str, Any]:
    """Record runtime, CPU and a fingerprint of the measured production source."""
    digest = hashlib.sha256()
    for path in sorted((root / "src").rglob("*.py")):
        digest.update(str(path.relative_to(root)).encode())
        digest.update(path.read_bytes())
    cpuinfo = Path("/proc/cpuinfo")
    cpu = platform.processor()
    if cpuinfo.exists():
        cpu = next(
            (
                line.split(":", 1)[1].strip()
                for line in cpuinfo.read_text().splitlines()
                if line.startswith("model name")
            ),
            cpu,
        )
    return {
        "recorded_at_utc": datetime.now(UTC).isoformat(),
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "cpu": cpu,
        "logical_cpus": os.cpu_count(),
        "available_cpus": len(os.sched_getaffinity(0))
        if hasattr(os, "sched_getaffinity")
        else os.cpu_count(),
        "event_loop": type(asyncio.get_running_loop()).__name__,
        "event_loop_debug": asyncio.get_running_loop().get_debug(),
        "gc_enabled": gc.isenabled(),
        "collect_before_trial": True,
        "python_hash_seed": os.environ.get("PYTHONHASHSEED", "randomized"),
        "assertions_enabled": __debug__,
        "source_sha256": digest.hexdigest(),
        "benchmark_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }


async def main() -> None:
    """Run workloads sequentially and save every measured trial to JSON."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("benchmarks/results.json"))
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--core-operations", type=int, default=100_000)
    parser.add_argument("--network-operations", type=int, default=5_000)
    parser.add_argument("--publications", type=int, default=1_000)
    args = parser.parse_args()
    if (
        min(
            args.repeats,
            args.core_operations,
            args.network_operations,
            args.publications,
        )
        < 1
        or args.warmups < 0
    ):
        parser.error("counts must be positive and warmups nonnegative")
    report: dict[str, Any] = {
        "environment": environment(_ROOT),
        "configuration": {
            key: str(value) if isinstance(value, Path) else value
            for key, value in vars(args).items()
        },
        "results": {},
    }
    for case in cases(args):
        for _ in range(args.warmups):
            gc.collect()
            async with asyncio.timeout(120):
                await case.run()
        trials = []
        for _ in range(args.repeats):
            gc.collect()
            async with asyncio.timeout(120):
                trials.append(await case.run())
        result = {"parameters": case.parameters, **summarize(trials)}
        report["results"][case.name] = result
        print(
            f"{case.name}: {result['median_operations_per_second']:,.0f} ops/s",
            flush=True,
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Saved {args.output}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
