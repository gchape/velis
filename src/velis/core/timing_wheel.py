"""Hierarchical timing wheel with exact deadlines and bounded maintenance."""

from collections import OrderedDict
from dataclasses import dataclass, field

_BITS = 6
_SLOTS = 1 << _BITS
_MASK = _SLOTS - 1


@dataclass(slots=True, eq=False)
class _Bucket:
    level: int
    slot: int
    keys: OrderedDict[str, None] = field(default_factory=OrderedDict)


@dataclass(frozen=True, slots=True)
class _Timer:
    deadline: int
    bucket: _Bucket


class TimingWheel:
    """Schedule exact millisecond deadlines in a sparse 64-slot-per-level wheel.

    Each level spans 64 times more time than the previous one. Advancing scans
    at most 64 slots per level, even across a large clock jump. Ready buckets
    are drained within the caller's timer budget; future timers cascade into
    finer levels. Exact deadlines are always checked before expiration.

    Each key has one timer. Rescheduling and cancellation unlink it immediately,
    including from partly drained buckets, without stale entries or compaction.
    """

    def __init__(self, now: int = 0) -> None:
        self._now = now
        self._timers: dict[str, _Timer] = {}
        self._wheel: list[dict[int, _Bucket]] = []
        self._ready: OrderedDict[_Bucket, None] = OrderedDict()
        self._immediate = _Bucket(-1, 0)

    def __len__(self) -> int:
        return len(self._timers)

    def deadline(self, key: str) -> int | None:
        """Return the exact scheduled deadline, or None when no timer exists."""
        timer = self._timers.get(key)
        return None if timer is None else timer.deadline

    def set(self, key: str, deadline: int) -> None:
        """Replace the key's timer, unlinking its previous bucket immediately."""
        self.clear(key)
        self._schedule(key, deadline)

    def _schedule(self, key: str, deadline: int) -> None:
        """Place a timer in the finest level covering its remaining delay."""
        delta = deadline - self._now
        if delta <= 0:
            bucket = self._immediate
            self._ready[bucket] = None
        else:
            level = (delta.bit_length() - 1) // _BITS
            while len(self._wheel) <= level:
                self._wheel.append({})
            slot = (deadline >> (level * _BITS)) & _MASK
            bucket = self._wheel[level].get(slot)
            if bucket is None:
                bucket = self._wheel[level][slot] = _Bucket(level, slot)
        bucket.keys[key] = None
        self._timers[key] = _Timer(deadline, bucket)

    def _unlink(self, key: str, timer: _Timer) -> None:
        """Unlink a timer and release its bucket when it becomes empty."""
        bucket = timer.bucket
        del bucket.keys[key]
        if not bucket.keys:
            self._ready.pop(bucket, None)
            if bucket.level >= 0:
                slots = self._wheel[bucket.level]
                if slots.get(bucket.slot) is bucket:
                    del slots[bucket.slot]

    def clear(self, key: str) -> bool:
        """Cancel a timer immediately and return whether it existed."""
        timer = self._timers.pop(key, None)
        if timer is None:
            return False
        self._unlink(key, timer)
        return True

    def _advance(self, now: int) -> None:
        """Move crossed buckets to the ready queue without processing timers."""
        if now <= self._now:
            return
        for level, slots in enumerate(self._wheel):
            if not slots:
                continue
            shift = level * _BITS
            previous, current = self._now >> shift, now >> shift
            elapsed = current - previous
            if elapsed >= _SLOTS:
                self._ready.update((bucket, None) for bucket in slots.values())
                slots.clear()
            else:
                for tick in range(previous + 1, current + 1):
                    bucket = slots.pop(tick & _MASK, None)
                    if bucket is not None:
                        self._ready[bucket] = None
        self._now = now

    def has_due(self, now: int) -> bool:
        """Whether due timers or bucket cascades need another maintenance round."""
        self._advance(now)
        return bool(self._ready)

    def pop_due(self, now: int, limit: int) -> list[str]:
        """Process at most limit timers, counting both expiration and cascading."""
        self._advance(now)
        due = []
        for _ in range(limit):
            if not self._ready:
                break
            bucket = next(iter(self._ready))
            key = next(iter(bucket.keys))
            timer = self._timers.pop(key)
            self._unlink(key, timer)
            if timer.deadline <= self._now:
                due.append(key)
            else:
                self._schedule(key, timer.deadline)
        return due
