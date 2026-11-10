"""Bounded key previews, activity records and session statistics."""

import time
from dataclasses import dataclass, field
from enum import Enum, auto

from .text import format_duration

PREVIEW_BYTES = 4096


class Kind(Enum):
    """Severity or source of an activity message."""

    INFO = auto()
    OK = auto()
    WARN = auto()
    ERROR = auto()
    PUSH = auto()


MARKS = {Kind.INFO: "·", Kind.OK: "✓", Kind.WARN: "!", Kind.ERROR: "✗", Kind.PUSH: "◂"}


class Status(Enum):
    """Last known existence of a tracked key."""

    UNKNOWN = auto()
    LIVE = auto()
    GONE = auto()


@dataclass(frozen=True, slots=True)
class Activity:
    """One timestamped, escaped message in the bounded activity log."""

    at: float
    when: str
    kind: Kind
    text: str


@dataclass(slots=True)
class Entry:
    """What this session last learned about one key.

    Only a bounded preview of the value is kept, so tracking many large values
    costs little memory. ``expires_at`` is a local monotonic deadline, which
    lets the display count down between background checks.
    """

    key: str
    status: Status = Status.UNKNOWN
    size: int | None = None
    preview: bytes = b""
    ttl_known: bool = False
    expires_at: float | None = None

    def set_value(self, value: bytes) -> None:
        """Keep the value's size and a bounded preview."""
        self.size = len(value)
        self.preview = value[:PREVIEW_BYTES]

    def set_ttl(self, ms: int | None, now: float) -> None:
        """Mark the key live with a deadline, or no expiry when ms is None."""
        self.status = Status.LIVE
        self.ttl_known = True
        self.expires_at = None if ms is None else now + ms / 1000

    def set_gone(self) -> None:
        """Mark the key missing and discard its stale value and TTL."""
        self.status = Status.GONE
        self.size = None
        self.preview = b""
        self.ttl_known = False
        self.expires_at = None


def ttl_text(entry: Entry, now: float) -> str:
    """Describe a key's last known TTL relative to a monotonic timestamp."""
    if entry.status is Status.GONE:
        return "gone"
    if not entry.ttl_known:
        return "?"
    if entry.expires_at is None:
        return "∞"
    return format_duration(max(0.0, entry.expires_at - now))


@dataclass(slots=True)
class Stats:
    """Counters for this client session, independent of the activity log.

    Operations include background refreshes and failed attempts; cancellation
    does not complete an operation. Latency measures execution, excluding queue
    time. Hits and misses count READ replies only, never TTL refreshes. These
    observations cannot determine server-wide evictions or expirations.
    """

    started_at: float = field(default_factory=time.monotonic)
    operations: int = 0
    errors: int = 0
    hits: int = 0
    misses: int = 0
    published: int = 0
    received: int = 0
    last_ms: float | None = None
    total_ms: float = 0.0

    def record_operation(self, elapsed_ms: float, *, failed: bool) -> None:
        """Record one completed operation and its execution time."""
        self.operations += 1
        self.errors += int(failed)
        self.last_ms = elapsed_ms
        self.total_ms += elapsed_ms

    @property
    def average_ms(self) -> float | None:
        """Return mean operation latency, or None before the first result."""
        return self.total_ms / self.operations if self.operations else None

    @property
    def hit_rate(self) -> float | None:
        """Return the READ hit percentage, or None before the first READ reply."""
        reads = self.hits + self.misses
        return self.hits * 100 / reads if reads else None
