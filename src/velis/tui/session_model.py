"""Bounded observations from one client session, independent of scheduling."""

from collections import OrderedDict
from dataclasses import dataclass, field

from .models import Entry, Stats, Status

MAX_TRACKED = 256


@dataclass(slots=True)
class SessionModel:
    """Own tracked values, watched channels and cumulative session counters."""

    entries: OrderedDict[str, Entry] = field(default_factory=OrderedDict[str, Entry])
    channels: set[str] = field(default_factory=set[str])
    stats: Stats = field(default_factory=Stats)

    def track(self, key: str) -> Entry:
        """Find or create a preview, discarding a gone key first at capacity."""
        entry = self.entries.get(key)
        if entry is None:
            if len(self.entries) >= MAX_TRACKED:
                victim = next(
                    (k for k, e in self.entries.items() if e.status is Status.GONE),
                    next(iter(self.entries)),
                )
                del self.entries[victim]
            entry = self.entries[key] = Entry(key)
        return entry

    def forget(self, key: str) -> int:
        """Forget one preview, or all gone previews; return the number removed."""
        if key:
            return int(self.entries.pop(key, None) is not None)
        gone = [
            key for key, entry in self.entries.items() if entry.status is Status.GONE
        ]
        for key in gone:
            del self.entries[key]
        return len(gone)
