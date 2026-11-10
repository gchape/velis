"""Velis benchmarks: TCP throughput, hot-path costs and expiry indexes.

Run ``python benchmarks/bench.py <command> --help`` for each command's options.

Commands:

tcp        Compare Client.batch, concurrent client calls and raw TCP pipeline
           windows. The server runs in a separate process. Each sample opens a
           connection, warms it up, then times completed operations with every
           result checked. Client modes include encoding and decoding; raw mode
           pre-encodes its frames outside timing. Depth one measures sequential
           requests. Larger depths measure bounded windows.
hot-paths  Isolate codec, server dispatch and expiry costs without socket
           scheduling. Each sample constructs fresh state before timing. Checks
           run in the measured loop; tracemalloc runs separately and reports
           incremental peak allocations. Use ``tcp`` to check whether local
           gains survive real network I/O.
expiry     Compare the production expiry heap with SortedList and heapdict
           adapters. Needs sortedcontainers and heapdict installed. Setup is
           untimed; correctness checks are timed. The adapters are research
           prototypes, not alternate production indexes or latency guarantees.

Every command prints a JSON report to stdout and can also save it with --output.
"""

import argparse
import asyncio
import json
import logging
import multiprocessing
import platform
import statistics
import subprocess
import sys
import tracemalloc
from collections.abc import Callable, Generator
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from importlib import import_module
from importlib.metadata import version
from multiprocessing.connection import Connection as PipeConnection
from pathlib import Path
from time import perf_counter
from typing import Any

from velis.app import serve
from velis.config import (
    DEFAULT_SESSION_LIMITS,
    DEFAULT_STORE_LIMITS,
    ServerLimits,
    SessionLimits,
)
from velis.core.broker import Broker
from velis.core.engine import Engine
from velis.core.expiry_index import ExpiryIndex
from velis.core.messages import NIL, OK, Nil, Ok, Read, Value, Write
from velis.core.store import Store
from velis.net.client import Client
from velis.net.connection import Connection
from velis.net.limits import MAX_FIELD_BYTES
from velis.net.protocol import encode_command, encode_message


def _cpu_model() -> str:
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.is_file():
        for line in cpuinfo.read_text().splitlines():
            if line.startswith("model name"):
                return line.partition(":")[2].strip()
    return platform.processor()


def _git(*args: str) -> str | None:
    try:
        return subprocess.run(
            ["git", *args], check=True, capture_output=True, text=True
        ).stdout.strip()
    except OSError, subprocess.CalledProcessError:
        return None


def _environment() -> dict[str, object]:
    """Describe where and when a report was produced. Git fields are optional."""
    status = _git("status", "--porcelain")
    return {
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "commit": _git("rev-parse", "HEAD"),
        "dirty_worktree": None if status is None else bool(status),
        "python": sys.version,
        "platform": platform.platform(),
        "cpu": _cpu_model(),
    }


def _emit(report: dict[str, object], output: Path | None) -> None:
    encoded = json.dumps(report, indent=2) + "\n"
    if output is not None:
        output.write_text(encoded)
    print(encoded, end="")


KEY_COUNT = 10_000
type CacheCommand = Read | Write
type Windows = list[tuple[CacheCommand, ...]]


@dataclass(frozen=True, slots=True)
class Workload:
    name: str
    commands: tuple[CacheCommand, ...]
    reply: Ok | Nil | Value


def _key(prefix: str, index: int) -> str:
    return f"{prefix}{index:015d}"


async def _serve(control: PipeConnection, value_bytes: int, key_count: int) -> None:
    store = Store()
    value = b"x" * value_bytes
    for index in range(key_count):
        store.set(_key("k", index), value)
    async with serve(ServerLimits(port=0), engine=Engine(store, Broker())) as server:
        control.send(server.port)
        await asyncio.to_thread(control.recv)


def _server_process(control: PipeConnection, value_bytes: int, key_count: int) -> None:
    logging.basicConfig(level=logging.WARNING)
    try:
        asyncio.run(_serve(control, value_bytes, key_count))
    finally:
        control.close()


@contextmanager
def _server(value_bytes: int, key_count: int) -> Generator[int]:
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(
        target=_server_process, args=(child, value_bytes, key_count)
    )
    process.start()
    child.close()
    try:
        if not parent.poll(30):
            raise TimeoutError("benchmark server did not start within 30 seconds")
        yield parent.recv()
    finally:
        if process.is_alive():
            with suppress(BrokenPipeError):
                parent.send(None)
            process.join(10)
        if process.is_alive():
            process.terminate()
            process.join(10)
        if process.is_alive():
            process.kill()
            process.join(10)
        exitcode = process.exitcode
        parent.close()
        process.close()
    if exitcode != 0:
        raise RuntimeError(f"benchmark server exited with {exitcode}")


def _workloads(value_bytes: int, key_count: int) -> tuple[Workload, ...]:
    value = b"x" * value_bytes
    return (
        Workload(
            "WRITE overwrite",
            tuple(Write(_key("k", i), value) for i in range(key_count)),
            OK,
        ),
        Workload(
            "READ hit",
            tuple(Read(_key("k", i)) for i in range(key_count)),
            Value(value),
        ),
        Workload("READ miss", tuple(Read(_key("m", i)) for i in range(key_count)), NIL),
    )


def _windows(workload: Workload, count: int, depth: int) -> Windows:
    return [
        tuple(
            workload.commands[i % len(workload.commands)]
            for i in range(start, min(start + depth, count))
        )
        for start in range(0, count, depth)
    ]


def _wire_windows(workload: Workload, windows: Windows) -> list[tuple[bytes, bytes]]:
    reply = encode_message(workload.reply)
    return [
        (b"".join(map(encode_command, commands)), reply * len(commands))
        for commands in windows
    ]


async def _exchange_raw(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    windows: list[tuple[bytes, bytes]],
) -> float:
    """Time raw exchanges, reading before drain to avoid mutual backpressure."""
    start = perf_counter()
    for request, expected in windows:
        writer.write(request)
        actual = await reader.readexactly(len(expected))
        if actual != expected:
            raise RuntimeError("raw server reply differs from the expected wire bytes")
        await writer.drain()
    return perf_counter() - start


async def _exchange_client(
    client: Client, workload: Workload, windows: Windows, mode: str
) -> float:
    expected_value = workload.reply.value if isinstance(workload.reply, Value) else None
    start = perf_counter()
    for commands in windows:
        if mode == "batch":
            replies = await client.batch(commands)
            if replies != [workload.reply] * len(commands):
                raise RuntimeError("batch replies differ from the expected results")
        else:
            values = await asyncio.gather(
                *(
                    client.write(command.key, command.value)
                    if isinstance(command, Write)
                    else client.read(command.key)
                    for command in commands
                )
            )
            if values != [expected_value] * len(commands):
                raise RuntimeError(
                    "concurrent replies differ from the expected results"
                )
    return perf_counter() - start


async def _measure(
    port: int,
    workload: Workload,
    mode: str,
    windows: Windows,
    warmup: Windows,
    limits: SessionLimits,
) -> float:
    if mode == "raw":
        wire = _wire_windows(workload, windows)
        warm = _wire_windows(workload, warmup)
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            async with asyncio.timeout(120):
                await _exchange_raw(reader, writer, warm)
                return await _exchange_raw(reader, writer, wire)
        finally:
            writer.close()
            await writer.wait_closed()
    async with await Client.connect("127.0.0.1", port, limits=limits) as client:
        async with asyncio.timeout(120):
            await _exchange_client(client, workload, warmup, mode)
            return await _exchange_client(client, workload, windows, mode)


async def _benchmark_tcp(
    port: int, args: argparse.Namespace, limits: SessionLimits
) -> list[dict[str, object]]:
    cases = [
        (workload, mode, depth)
        for workload in _workloads(args.value_bytes, args.keys)
        for mode in args.mode
        for depth in args.pipeline
    ]
    samples: dict[tuple[str, str, int], list[float]] = {
        (workload.name, mode, depth): [] for workload, mode, depth in cases
    }
    for repeat in range(args.repeats):
        ordered = cases if repeat % 2 == 0 else list(reversed(cases))
        for workload, mode, depth in ordered:
            windows = _windows(workload, args.requests, depth)
            warmup = _windows(workload, args.warmup, depth)
            elapsed = await _measure(port, workload, mode, windows, warmup, limits)
            rate = args.requests / elapsed
            samples[workload.name, mode, depth].append(rate)
            print(
                f"{repeat + 1}/{args.repeats} {workload.name}, "
                f"{mode}, depth {depth}: {rate:,.0f} ops/s",
                file=sys.stderr,
                flush=True,
            )
    return [
        {
            "workload": name,
            "mode": mode,
            "pipeline": depth,
            "median_ops_per_second": round(statistics.median(rates)),
            "min_ops_per_second": round(min(rates)),
            "max_ops_per_second": round(max(rates)),
            "samples_ops_per_second": [round(rate) for rate in rates],
        }
        for (name, mode, depth), rates in samples.items()
    ]


def _add_tcp_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--requests", type=int, default=20_000)
    parser.add_argument("--warmup", type=int, default=2_000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--pipeline", type=int, nargs="+", default=[1, 64, 256])
    parser.add_argument(
        "--mode",
        choices=("batch", "concurrent", "raw"),
        nargs="+",
        default=["batch", "concurrent"],
    )
    parser.add_argument("--value-bytes", type=int, default=64)
    parser.add_argument("--keys", type=int, default=KEY_COUNT)
    parser.add_argument(
        "--pending-requests",
        type=int,
        default=DEFAULT_SESSION_LIMITS.max_pending_requests,
    )
    parser.add_argument(
        "--batch-bytes", type=int, default=DEFAULT_SESSION_LIMITS.max_batch_bytes
    )


def _validate_tcp(args: argparse.Namespace) -> None:
    parser: argparse.ArgumentParser = args.parser
    if (
        min(
            args.requests,
            args.repeats,
            args.pending_requests,
            args.batch_bytes,
            args.keys,
            *args.pipeline,
        )
        <= 0
    ):
        parser.error(
            "requests, keys, repeats, limits and pipeline depths must be positive"
        )
    if args.warmup < 0:
        parser.error("warmup must be nonnegative")
    if args.keys > DEFAULT_STORE_LIMITS.max_keys:
        parser.error("keys must fit the store key limit")
    max_value = min(MAX_FIELD_BYTES, DEFAULT_STORE_LIMITS.max_bytes // args.keys - 16)
    if not 0 <= args.value_bytes <= max_value:
        parser.error(
            f"value-bytes must be in 0..{max_value} so the working set fits the store"
        )
    if len(set(args.pipeline)) != len(args.pipeline) or len(set(args.mode)) != len(
        args.mode
    ):
        parser.error("pipeline depths and modes must be distinct")


def _run_tcp(args: argparse.Namespace) -> dict[str, object]:
    _validate_tcp(args)
    limits = SessionLimits(
        max_pending_requests=args.pending_requests, max_batch_bytes=args.batch_bytes
    )
    with _server(args.value_bytes, args.keys) as port:
        results = asyncio.run(_benchmark_tcp(port, args, limits))
    return {
        "benchmark": "tcp",
        **_environment(),
        "event_loop": "asyncio default",
        "server_processes": 1,
        "client_processes": 1,
        "connections_per_sample": 1,
        "key_bytes": 16,
        "resident_keys": args.keys,
        "eviction_policy": DEFAULT_STORE_LIMITS.eviction_policy.value,
        "session_limits": asdict(limits),
        "requests": args.requests,
        "warmup": args.warmup,
        "repeats": args.repeats,
        "pipeline": args.pipeline,
        "modes": args.mode,
        "value_bytes": args.value_bytes,
        "results": results,
    }


class CheckedTransport(asyncio.Transport):
    """Check replies without adding socket or event-loop overhead."""

    def __init__(self, expected: bytes) -> None:
        super().__init__()
        self.expected = expected
        self.writes = 0

    def is_closing(self) -> bool:
        return False

    def write(self, data: bytes | bytearray | memoryview) -> None:
        if data != self.expected:
            raise AssertionError("unexpected server response")
        self.writes += 1


@dataclass(frozen=True)
class Case:
    name: str
    prepare: Callable[[], Callable[[], None]]
    operations: int


def _encoding(size: int, count: int) -> Callable[[], None]:
    command = Write("key", b"x" * size)
    expected = encode_command(command)

    def run() -> None:
        for _ in range(count):
            if encode_command(command) != expected:
                raise AssertionError("encoding changed")

    return run


def _dispatch(size: int, depth: int, count: int) -> Callable[[], None]:
    frame = encode_command(Write("key", b"x" * size)) * depth
    transport = CheckedTransport(encode_message(OK) * depth)
    connection = Connection(
        Engine(Store(), Broker()), 8 << 20, admit=lambda: True, release=lambda: None
    )
    connection.connection_made(transport)

    def run() -> None:
        writes = transport.writes
        for _ in range(count):
            connection.data_received(frame)
        if transport.writes - writes != count:
            raise AssertionError("missing replies")

    return run


def _refresh(count: int, *, same_deadline: bool) -> Callable[[], None]:
    index = ExpiryIndex()
    index.set("key", 1_000_000)

    def run() -> None:
        for i in range(count):
            index.set("key", 1_000_000 if same_deadline else i)
        expected = 1_000_000 if same_deadline else count - 1
        if index.deadline("key") != expected:
            raise AssertionError("deadline changed")

    return run


def _future_probe() -> Callable[[], None]:
    index = ExpiryIndex()
    for i in range(100_000):
        index.set(str(i), i + 1)
    for i in range(100_000):
        index.set(str(i), i + 200_000)

    def run() -> None:
        if index.has_due(0):
            raise AssertionError("future timer reported due")

    return run


HOT_PATH_CASES = (
    Case("encode WRITE 64 B", lambda: _encoding(64, 100_000), 100_000),
    Case("encode WRITE 1 MiB", lambda: _encoding(1 << 20, 500), 500),
    Case("dispatch WRITE 64 B, depth 32", lambda: _dispatch(64, 32, 1000), 32_000),
    Case("dispatch WRITE 1 MiB", lambda: _dispatch(1 << 20, 1, 500), 500),
    Case(
        "refresh unchanged TTL", lambda: _refresh(100_000, same_deadline=True), 100_000
    ),
    Case(
        "refresh changing TTL", lambda: _refresh(100_000, same_deadline=False), 100_000
    ),
    Case("probe 100k future stale timers", _future_probe, 1),
)


def _run_hot_paths(args: argparse.Namespace) -> dict[str, object]:
    """Measure isolated cases after warming interpreter specialization."""
    if args.repeats <= 0:
        args.parser.error("repeats must be positive")
    results = []
    for case in HOT_PATH_CASES:
        case.prepare()()
        elapsed = []
        for _ in range(args.repeats):
            run = case.prepare()
            start = perf_counter()
            run()
            elapsed.append(perf_counter() - start)
        run = case.prepare()
        tracemalloc.start()
        run()
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        results.append(
            {
                "case": case.name,
                "operations": case.operations,
                "seconds": elapsed,
                "median_ops_per_second": case.operations / statistics.median(elapsed),
                "peak_traced_bytes": peak,
            }
        )
        print(f"measured {case.name}", file=sys.stderr)
    return {
        "benchmark": "hot-paths",
        **_environment(),
        "repeats": args.repeats,
        "results": results,
    }


EXPIRY_KEYS = tuple(str(i) for i in range(20_000))
EXPIRY_OPERATIONS = ("reschedule", "cancel", "expire", "stale due probe")


class SortedExpiry:
    """Sorted-sequence adapter over ``sortedcontainers.SortedList``."""

    def __init__(self, sorted_list: Callable[[], Any]) -> None:
        self.deadlines: dict[str, int] = {}
        self.order = sorted_list()

    def set(self, key: str, deadline: int) -> None:
        old = self.deadlines.get(key)
        if old == deadline:
            return
        if old is not None:
            self.order.remove((old, key))
        self.deadlines[key] = deadline
        self.order.add((deadline, key))

    def clear(self, key: str) -> bool:
        old = self.deadlines.pop(key, None)
        if old is None:
            return False
        self.order.remove((old, key))
        return True

    def has_due(self, now: int) -> bool:
        return bool(self.order) and self.order[0][0] <= now

    def pop_due(self, now: int, limit: int) -> list[str]:
        due: list[str] = []
        while len(due) < limit and self.has_due(now):
            _, key = self.order.pop(0)
            del self.deadlines[key]
            due.append(key)
        return due


class IndexedExpiry:
    """Indexed priority-queue adapter over ``heapdict.heapdict``."""

    def __init__(self, heapdict: Callable[[], Any]) -> None:
        self.order = heapdict()

    def set(self, key: str, deadline: int) -> None:
        value = (deadline, key)
        if self.order.get(key) != value:
            self.order[key] = value

    def clear(self, key: str) -> bool:
        return self.order.pop(key, None) is not None

    def has_due(self, now: int) -> bool:
        return bool(self.order) and self.order.peekitem()[1][0] <= now

    def pop_due(self, now: int, limit: int) -> list[str]:
        due: list[str] = []
        while len(due) < limit and self.has_due(now):
            due.append(self.order.popitem()[0])
        return due


def _expiry_factories() -> dict[str, Callable[[], Any]]:
    try:
        sorted_list = import_module("sortedcontainers").SortedList
        heapdict = import_module("heapdict").heapdict
    except ModuleNotFoundError as error:
        raise SystemExit(
            f"expiry needs sortedcontainers and heapdict installed: {error}"
        ) from error
    return {
        "ExpiryIndex": ExpiryIndex,
        "SortedExpiry": lambda: SortedExpiry(sorted_list),
        "IndexedExpiry": lambda: IndexedExpiry(heapdict),
    }


def _time_expiry(factory: Callable[[], Any], operation: str) -> float:
    index = factory()
    for i, key in enumerate(EXPIRY_KEYS):
        index.set(key, i)
    if operation == "stale due probe":
        for i, key in enumerate(EXPIRY_KEYS):
            index.set(key, i + len(EXPIRY_KEYS))
    start = perf_counter()
    if operation == "reschedule":
        for i, key in enumerate(EXPIRY_KEYS):
            index.set(key, i + len(EXPIRY_KEYS))
    elif operation == "cancel":
        for key in EXPIRY_KEYS:
            assert index.clear(key)
    elif operation == "expire":
        due = index.pop_due(len(EXPIRY_KEYS), len(EXPIRY_KEYS))
        assert due == list(EXPIRY_KEYS)
    else:
        assert not index.has_due(len(EXPIRY_KEYS) - 1)
    return perf_counter() - start


def _run_expiry(args: argparse.Namespace) -> dict[str, object]:
    if args.repeats <= 0:
        args.parser.error("repeats must be positive")
    factories = _expiry_factories()
    results = []
    for operation in EXPIRY_OPERATIONS:
        for name, factory in factories.items():
            samples = [_time_expiry(factory, operation) for _ in range(args.repeats)]
            results.append(
                {
                    "index": name,
                    "operation": operation,
                    "seconds": samples,
                    "median_seconds": statistics.median(samples),
                }
            )
            print(f"measured {name}, {operation}", file=sys.stderr)
    return {
        "benchmark": "expiry",
        **_environment(),
        "versions": {n: version(n) for n in ("heapdict", "sortedcontainers")},
        "keys": len(EXPIRY_KEYS),
        "repeats": args.repeats,
        "results": results,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--output", type=Path, help="also save the JSON report to this path"
    )
    commands = parser.add_subparsers(dest="command", required=True)

    tcp = commands.add_parser(
        "tcp", parents=[common], help="Client.batch, concurrent and raw TCP windows"
    )
    _add_tcp_arguments(tcp)
    tcp.set_defaults(run=_run_tcp, parser=tcp)

    hot_paths = commands.add_parser(
        "hot-paths", parents=[common], help="codec, dispatch and TTL costs"
    )
    hot_paths.add_argument("--repeats", type=int, default=5)
    hot_paths.set_defaults(run=_run_hot_paths, parser=hot_paths)

    expiry = commands.add_parser(
        "expiry",
        parents=[common],
        help="expiry heap versus sortedcontainers and heapdict adapters",
    )
    expiry.add_argument("--repeats", type=int, default=5)
    expiry.set_defaults(run=_run_expiry, parser=expiry)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    _emit(args.run(args), args.output)


if __name__ == "__main__":
    main()
