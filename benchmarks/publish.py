"""Compare per-subscriber and shared publish encoding without socket I/O.

Run: uv run python benchmarks/publish.py --subscribers 1000 --repeats 3
"""

import argparse
import asyncio
from contextlib import nullcontext
from statistics import median
from time import perf_counter
from typing import cast

from velis.core.broker import Broker
from velis.net.connection import Connection, share_message_encodings


class Sink:
    """Accept writes without retaining buffers or simulating network throughput."""

    @property
    def transport(self) -> Sink:
        return self

    def is_closing(self) -> bool:
        return False

    def write(self, data: bytes) -> None:
        pass

    def get_write_buffer_size(self) -> int:
        return 0


def measure(broker: Broker, payload: bytes, shared: bool) -> float:
    start = perf_counter()
    with share_message_encodings() if shared else nullcontext():
        broker.publish("news", payload)
    return (perf_counter() - start) * 1000


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subscribers", type=int, default=1000)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if args.subscribers <= 0 or args.repeats <= 0:
        parser.error("subscribers and repeats must be positive")
    broker = Broker()
    writer = cast(asyncio.StreamWriter, Sink())
    for _ in range(args.subscribers):
        connection = Connection(cast(asyncio.StreamReader, None), writer, 8 << 20)
        broker.subscribe("news", connection)
    for size in (64, 1 << 20):
        payload = b"x" * size
        repeated = median(measure(broker, payload, False) for _ in range(args.repeats))
        shared = median(measure(broker, payload, True) for _ in range(args.repeats))
        print(
            f"{args.subscribers} subscribers, {size} bytes: "
            f"per-subscriber {repeated:.3f} ms, shared {shared:.3f} ms "
            f"({repeated / shared:.1f}x)"
        )


if __name__ == "__main__":
    main()
