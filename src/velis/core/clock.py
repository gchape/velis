from __future__ import annotations

import time
from typing import Protocol, final


class Clock(Protocol):
    """Time source for TTL calculation and expiry checks."""

    def now_ms(self) -> int:
        """Wall-clock unix time in milliseconds."""
        ...


@final
class SystemClock:
    """Read the system wall clock without instance state."""

    @staticmethod
    def now_ms() -> int:
        """Return the current Unix time in milliseconds."""
        return time.time_ns() // 1_000_000
