"""Measure the current server over localhost TCP with a separate load process.

Requests are encoded before timing. Every reply is checked against its expected
frame inside the timed interval. Each sample uses a new connection and an untimed
warmup. SET overwrites and GET hits cycle through 10,000 preloaded 16-byte keys;
GET misses use a disjoint key prefix. No TTLs or evictions are exercised.
"""

import argparse
import asyncio
import json
import logging
import multiprocessing
import platform
import statistics
import sys
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from multiprocessing.connection import Connection
from pathlib import Path
from time import perf_counter

from velis.app import serve
from velis.core.broker import Broker
from velis.core.engine import Engine
from velis.core.messages import NIL, OK, Get, Set, Value
from velis.core.store import DEFAULT_STORE_LIMITS, Store
from velis.net.protocol import encode_command, encode_message
from velis.net.server import ServerLimits

KEY_COUNT = 10_000


@dataclass(frozen=True, slots=True)
class Workload:
    name: str
    requests: tuple[bytes, ...]
    reply: bytes


def _key(prefix: str, index: int) -> str:
    return f"{prefix}{index:015d}"


async def _serve(control: Connection, value_bytes: int) -> None:
    store = Store()
    for index in range(KEY_COUNT):
        store.set(_key("k", index), b"x" * value_bytes)
    engine = Engine(store, Broker())
    async with serve(ServerLimits(port=0), engine=engine) as server:
        control.send(server.port)
        await asyncio.to_thread(control.recv)


def _server_process(control: Connection, value_bytes: int) -> None:
    logging.basicConfig(level=logging.WARNING)
    try:
        asyncio.run(_serve(control, value_bytes))
    finally:
        control.close()


@contextmanager
def _server(value_bytes: int) -> Generator[int]:
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(target=_server_process, args=(child, value_bytes))
    process.start()
    child.close()
    try:
        if not parent.poll(30):
            raise TimeoutError("benchmark server did not start within 30 seconds")
        yield parent.recv()
    finally:
        if process.is_alive():
            parent.send(None)
            process.join(10)
        if process.is_alive():
            process.terminate()
            process.join()
        parent.close()
    if process.exitcode != 0:
        raise RuntimeError(f"benchmark server exited with {process.exitcode}")


def _workloads(value_bytes: int) -> tuple[Workload, ...]:
    value = b"x" * value_bytes
    return (
        Workload(
            "SET overwrite",
            tuple(
                encode_command(Set(_key("k", index), value))
                for index in range(KEY_COUNT)
            ),
            encode_message(OK),
        ),
        Workload(
            "GET hit",
            tuple(encode_command(Get(_key("k", index))) for index in range(KEY_COUNT)),
            encode_message(Value(value)),
        ),
        Workload(
            "GET miss",
            tuple(encode_command(Get(_key("m", index))) for index in range(KEY_COUNT)),
            encode_message(NIL),
        ),
    )


def _batches(
    workload: Workload, count: int, pipeline: int
) -> list[tuple[bytes, bytes]]:
    requests = workload.requests
    return [
        (
            b"".join(requests[index % len(requests)] for index in range(start, end)),
            workload.reply * (end - start),
        )
        for start in range(0, count, pipeline)
        for end in (min(start + pipeline, count),)
    ]


async def _exchange(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    batches: list[tuple[bytes, bytes]],
) -> float:
    """Drain each pipeline window before sending the next, verifying all replies.

    Read before awaiting drain so large windows can make progress in both
    directions even when the client and server both encounter backpressure.
    """
    start = perf_counter()
    for request, expected in batches:
        writer.write(request)
        actual = await reader.readexactly(len(expected))
        if actual != expected:
            raise RuntimeError("server reply differs from the expected wire bytes")
        await writer.drain()
    return perf_counter() - start


async def _measure(
    port: int,
    batches: list[tuple[bytes, bytes]],
    warmup: list[tuple[bytes, bytes]],
) -> float:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        async with asyncio.timeout(120):
            await _exchange(reader, writer, warmup)
            return await _exchange(reader, writer, batches)
    finally:
        writer.close()
        await writer.wait_closed()


async def _benchmark(port: int, args: argparse.Namespace) -> list[dict[str, object]]:
    cases = [
        (workload, pipeline)
        for workload in _workloads(args.value_bytes)
        for pipeline in args.pipeline
    ]
    samples: dict[tuple[str, int], list[float]] = {
        (workload.name, pipeline): [] for workload, pipeline in cases
    }
    for repeat in range(args.repeats):
        ordered = cases if repeat % 2 == 0 else list(reversed(cases))
        for workload, pipeline in ordered:
            batches = _batches(workload, args.requests, pipeline)
            warmup = _batches(workload, args.warmup, pipeline)
            elapsed = await _measure(port, batches, warmup)
            rate = args.requests / elapsed
            samples[workload.name, pipeline].append(rate)
            print(
                f"{repeat + 1}/{args.repeats} {workload.name}, "
                f"pipeline {pipeline}: {rate:,.0f} ops/s",
                file=sys.stderr,
                flush=True,
            )
    return [
        {
            "workload": name,
            "pipeline": pipeline,
            "median_ops_per_second": round(statistics.median(rates)),
            "min_ops_per_second": round(min(rates)),
            "max_ops_per_second": round(max(rates)),
            "samples_ops_per_second": [round(rate) for rate in rates],
        }
        for (name, pipeline), rates in samples.items()
    ]


def _cpu_model() -> str:
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.is_file():
        for line in cpuinfo.read_text().splitlines():
            if line.startswith("model name"):
                return line.partition(":")[2].strip()
    return platform.processor()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=int, default=100_000)
    parser.add_argument("--warmup", type=int, default=10_000)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--pipeline", type=int, nargs="+", default=[1, 64, 1024])
    parser.add_argument("--value-bytes", type=int, default=64)
    args = parser.parse_args()
    if min(args.requests, args.warmup, args.repeats, *args.pipeline) <= 0:
        parser.error("requests, warmup, repeats and pipeline depths must be positive")
    if not 0 <= args.value_bytes <= (DEFAULT_STORE_LIMITS.max_bytes // KEY_COUNT) - 16:
        parser.error("the preloaded keys and values must fit the default store limits")
    if len(set(args.pipeline)) != len(args.pipeline):
        parser.error("pipeline depths must be distinct")
    with _server(args.value_bytes) as port:
        results = asyncio.run(_benchmark(port, args))
    print(
        json.dumps(
            {
                "timestamp_utc": datetime.now(UTC).isoformat(),
                "python": sys.version,
                "platform": platform.platform(),
                "cpu": _cpu_model(),
                "event_loop": "asyncio default",
                "server_processes": 1,
                "client_processes": 1,
                "connections_per_sample": 1,
                "key_bytes": 16,
                "resident_keys": KEY_COUNT,
                "eviction_policy": DEFAULT_STORE_LIMITS.eviction_policy.value,
                **vars(args),
                "results": results,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
