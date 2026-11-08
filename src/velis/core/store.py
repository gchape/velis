import heapq

from velis.config import DEFAULT_MAX_BYTES, DEFAULT_MAX_KEYS
from velis.core.clock import Clock, SystemClock
from velis.errors import CapacityError

MAX_TTL_MS = 1 << 62


class Store:
    """Store opaque bytes and monotonic TTLs on one owning thread."""

    def __init__(
        self,
        clock: Clock | None = None,
        *,
        max_keys: int = DEFAULT_MAX_KEYS,
        max_bytes: int = DEFAULT_MAX_BYTES,
    ) -> None:
        if max_keys <= 0 or max_bytes <= 0:
            raise ValueError("store limits must be positive")
        self._clock: Clock = SystemClock() if clock is None else clock
        self._max_keys = max_keys
        self._max_bytes = max_bytes
        self._stored_bytes = 0
        self._values: dict[str, bytes] = {}
        self._deadlines: dict[str, int] = {}
        self._expiry_heap: list[tuple[int, str]] = []

    def __len__(self) -> int:
        """Return the stored key count without purging expired entries."""
        return len(self._values)

    def _remove(self, key: str) -> None:
        value = self._values.pop(key, None)
        if value is not None:
            self._stored_bytes -= len(key.encode()) + len(value)
        self._deadlines.pop(key, None)

    @property
    def stored_bytes(self) -> int:
        """Retained UTF-8 key and value bytes, excluding Python object overhead."""
        return self._stored_bytes

    @property
    def has_due_expirations(self) -> bool:
        """Whether another bounded sweep can process due or stale heap entries."""
        return bool(
            self._expiry_heap and self._expiry_heap[0][0] <= self._clock.now_ms()
        )

    def _remove_if_expired(self, key: str) -> None:
        deadline = self._deadlines.get(key)
        if deadline is not None and deadline <= self._clock.now_ms():
            self._remove(key)

    def get(self, key: str) -> bytes | None:
        """Return a live value, or None if the key is missing or expired."""
        self._remove_if_expired(key)
        return self._values.get(key)

    def set(self, key: str, value: bytes) -> None:
        """Store a value and clear its TTL, or reject it without changing state.

        Capacity counts retained entries, including expired keys awaiting cleanup.
        No live entry is evicted to make room.
        """
        previous = self._values.get(key)
        size = len(key.encode()) + len(value)
        if previous is None:
            if len(self._values) >= self._max_keys:
                raise CapacityError("store key limit exceeded")
            stored_bytes = self._stored_bytes + size
        else:
            stored_bytes = self._stored_bytes - len(previous) + len(value)
        if stored_bytes > self._max_bytes:
            raise CapacityError("store byte limit exceeded")
        self._values[key] = value
        self._stored_bytes = stored_bytes
        self._deadlines.pop(key, None)
        self._compact_expiry_heap()

    def expire(self, key: str, ms: int) -> bool:
        """Set a relative TTL on a live key; expired keys stay missing.

        Raise ValueError for TTLs outside 0..MAX_TTL_MS, even for missing keys.
        Invalid TTLs leave the value and its existing expiration unchanged.
        """
        if not 0 <= ms <= MAX_TTL_MS:
            raise ValueError("ttl out of range")
        self._remove_if_expired(key)
        if key not in self._values:
            return False
        deadline = self._clock.now_ms() + ms
        self._deadlines[key] = deadline
        heapq.heappush(self._expiry_heap, (deadline, key))
        self._compact_expiry_heap()
        return True

    def _compact_expiry_heap(self) -> None:
        """Bound stale expiry entries relative to the number of active TTLs."""
        if len(self._expiry_heap) > max(1024, 2 * len(self._deadlines)):
            self._expiry_heap = [
                (deadline, key) for key, deadline in self._deadlines.items()
            ]
            heapq.heapify(self._expiry_heap)

    def sweep(self, limit: int = 1000) -> int:
        """Remove due keys and return the number removed.

        Process at most limit heap entries, including stale entries left by
        overwritten values or TTLs. Rebuild an oversized heap from current TTLs.
        """
        now_ms = self._clock.now_ms()
        removed = 0
        for _ in range(limit):
            if not self._expiry_heap or self._expiry_heap[0][0] > now_ms:
                break
            deadline, key = heapq.heappop(self._expiry_heap)
            if self._deadlines.get(key) == deadline:
                self._remove(key)
                removed += 1
        self._compact_expiry_heap()
        return removed
