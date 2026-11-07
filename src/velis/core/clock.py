from __future__ import annotations

import time
from typing import Protocol, final


class Clock(Protocol):
    """Time source for TTL calculation and expiry checks."""

    def now_ms(self) -> int:
        """Monotonic time in milliseconds, with an arbitrary origin."""
        ...


@final
class SystemClock:
    """Read the system monotonic clock without instance state."""

    @staticmethod
    def now_ms() -> int:
        """Return monotonic milliseconds for elapsed-time TTLs."""
        return time.monotonic_ns() // 1_000_000
