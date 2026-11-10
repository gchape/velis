"""Apply queued network commands to the terminal session's tracked state.

Operations translate successful replies into previews, subscriptions and activity.
The workspace owns scheduling, deadlines and error reporting; handlers let failures
propagate so every command receives the same lifecycle treatment.
"""

import time
from collections.abc import Callable

from velis.core.messages import MISSING, NO_EXPIRY
from velis.errors import CommandError
from velis.net.client import Client

from .commands import Request
from .discovery import DiscoveryOperations
from .models import Entry, Kind, Status
from .session_model import SessionModel
from .text import format_duration, label


class Operations:
    """Execute parsed requests without owning tasks, queues or terminal input."""

    def __init__(
        self, client: Client, model: SessionModel, log: Callable[[Kind, str], None]
    ) -> None:
        self._client = client
        self._model = model
        self._log = log
        self._discovery = DiscoveryOperations(client, model, log)

    async def execute(self, request: Request) -> None:
        """Dispatch a parsed network command to its operation handler."""
        handlers = {
            "ping": self._discovery.ping,
            "exists": self._discovery.exists,
            "size": self._discovery.size,
            "list": self._discovery.list_keys,
            "read": self._read,
            "write": self._write,
            "remove": self._remove,
            "lifetime": self._lifetime,
            "keep": self._keep,
            "remaining": self._remaining,
            "watch": self._subscription,
            "unwatch": self._subscription,
            "send": self._send,
            "sync": self._sync,
        }
        await handlers[request.name](request)

    async def _write(self, request: Request) -> None:
        """Store bytes and replace any cached value and TTL."""
        key = request.target
        await self._client.write(key, request.data)
        entry = self._model.track(key)
        entry.set_value(request.data)
        entry.set_ttl(None, time.monotonic())
        self._log(Kind.OK, f"Wrote {label(key)} ({len(request.data)} bytes)")

    async def _remove(self, request: Request) -> None:
        """Delete a key and mark its local preview gone."""
        key = request.target
        removed = await self._client.remove(key)
        self._model.track(key).set_gone()
        outcome = "deleted" if removed else "not found"
        self._log(Kind.OK if removed else Kind.INFO, f"Remove {label(key)} → {outcome}")

    async def _lifetime(self, request: Request) -> None:
        """Set a TTL and update the preview without reading the value."""
        key = request.target
        found = await self._client.lifetime(key, request.ms)
        self._apply_remaining(self._model.track(key), request.ms if found else MISSING)
        text = format_duration(request.ms / 1000) if found else "not found"
        self._log(Kind.OK if found else Kind.INFO, f"Lifetime {label(key)} → {text}")

    async def _keep(self, request: Request) -> None:
        """Remove a TTL, checking existence when the key was already persistent."""
        key = request.target
        if await self._client.keep(key):
            self._apply_remaining(self._model.track(key), NO_EXPIRY)
            self._log(Kind.OK, f"Keep {label(key)} → lifetime removed")
        else:
            self._apply_remaining(
                self._model.track(key), await self._client.remaining(key)
            )
            self._log(Kind.INFO, f"Keep {label(key)} → no lifetime to remove")

    async def _remaining(self, request: Request) -> None:
        """Fetch and display a key's TTL without counting a cache read."""
        key = request.target
        ttl = await self._client.remaining(key)
        self._apply_remaining(self._model.track(key), ttl)
        text = {MISSING: "missing", NO_EXPIRY: "no expiry"}.get(
            ttl, format_duration(ttl / 1000)
        )
        self._log(Kind.OK, f"Remaining {label(key)} → {text}")

    async def _subscription(self, request: Request) -> None:
        """Apply a subscription change after the server acknowledges it."""
        channel = request.target
        if request.name == "watch":
            await self._client.watch(channel)
            self._model.channels.add(channel)
            verb = "Watch"
        else:
            await self._client.unwatch(channel)
            self._model.channels.discard(channel)
            verb = "Unwatch"
        self._log(Kind.OK, f"{verb} {label(channel)}")

    async def _send(self, request: Request) -> None:
        """Publish bytes and count the acknowledged publication."""
        count = await self._client.send(request.target, request.data)
        self._model.stats.published += 1
        self._log(Kind.OK, f"Sent {label(request.target)} → {count} subscriber(s)")

    async def _read(self, request: Request) -> None:
        """Read a value and lifetime together, counting only the value lookup."""
        key = request.target
        snapshot = await self._client.read_entry(key)
        entry = self._model.track(key)
        raw = snapshot.value
        if raw is None:
            self._model.stats.misses += 1
            entry.set_gone()
            self._log(Kind.INFO, f"Read {label(key)} → not found")
        else:
            self._model.stats.hits += 1
            entry.set_value(raw)
            self._apply_remaining(entry, snapshot.remaining_ms)
            self._log(Kind.OK, f"Read {label(key)} → {len(raw)} bytes")

    def _apply_remaining(self, entry: Entry, ttl: int) -> None:
        """Apply a normalized TTL reply to a tracked key."""
        if ttl == MISSING:
            entry.set_gone()
        else:
            entry.set_ttl(None if ttl == NO_EXPIRY else ttl, time.monotonic())

    async def _sync(self, request: Request) -> None:
        """Re-check every tracked key's existence and TTL with one batch."""
        keys = list(self._model.entries)
        if not keys:
            return
        replies = await self._client.remaining_many(keys)
        failures = 0
        for key, reply in zip(keys, replies, strict=True):
            entry = self._model.entries.get(key)
            if entry is None:
                continue
            if isinstance(reply, CommandError):
                failures += 1
                self._log(Kind.ERROR, f"Sync {label(key)}: {reply}")
                continue
            ttl = reply
            was_live = entry.status is Status.LIVE
            self._apply_remaining(entry, ttl)
            if was_live and entry.status is Status.GONE:
                self._log(
                    Kind.INFO, f"gone: {label(key)} (expired, evicted or deleted)"
                )
        if failures:
            raise CommandError(f"sync completed with {failures} failed key(s)")
