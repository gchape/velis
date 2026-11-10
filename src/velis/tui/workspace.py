"""Serialize client operations and maintain tracked keys and session activity."""

import asyncio
import time
from collections import OrderedDict, deque
from collections.abc import Callable

from velis.core.messages import (
    MISSING,
    NO_EXPIRY,
    TTL,
    Err,
    Get,
    Nil,
    Persistent,
    Reply,
    TTLValue,
    Value,
)
from velis.errors import CommandError, ProtocolError
from velis.net.client import Client

from .commands import Request
from .models import Activity, Entry, Kind, Stats, Status
from .text import clean, format_duration, label, preview_text

MAX_TRACKED = 256
ACTIVITY_LINES = 500
QUEUE_SIZE = 32
OP_TIMEOUT_SECONDS = 10.0
REFRESH_SECONDS = 5.0
TICK_SECONDS = 0.5


def _ttl_of(reply: Reply) -> int:
    """Normalize a TTL-command reply to remaining ms, MISSING or NO_EXPIRY."""
    match reply:
        case TTLValue(ms):
            return ms
        case Persistent():
            return NO_EXPIRY
        case Nil():
            return MISSING
        case Err(message):
            raise CommandError(message)
        case _:
            raise ProtocolError(f"unexpected reply to TTL: {reply!r}")


class Workspace:
    """Own the client, the tracked keys and the activity log; no curses here.

    Operations run strictly in submission order through one bounded queue, so
    a reply is never applied out of order and a stalled server cannot make the
    client pile up work. ``changed`` is called whenever there is something new
    to draw.
    """

    def __init__(self, client: Client, *, read_only: bool = False) -> None:
        """Initialize bounded session state around an already connected client."""
        self.client = client
        self.read_only = read_only
        self.entries: OrderedDict[str, Entry] = OrderedDict()
        self.channels: set[str] = set()
        self.activity: deque[Activity] = deque(maxlen=ACTIVITY_LINES)
        self.connected = True
        self.stats = Stats()
        self.changed: Callable[[], None] = lambda: None
        self._queue: asyncio.Queue[Request] = asyncio.Queue(QUEUE_SIZE)
        self._last_refresh = time.monotonic()
        self._refresh_pending = False

    @property
    def pending(self) -> int:
        """Return queued operations, excluding the operation currently running."""
        return self._queue.qsize()

    def log(self, kind: Kind, text: str) -> None:
        """Append a bounded, escaped activity message and request a redraw."""
        stamp = time.strftime("%H:%M:%S")
        self.activity.append(Activity(time.monotonic(), stamp, kind, clean(text[:300])))
        self.changed()

    def submit(self, request: Request) -> bool:
        """Queue an operation; False when disconnected or too many are pending."""
        if not self.connected:
            return False
        if request.name == "refresh" and self._refresh_pending:
            return True
        try:
            self._queue.put_nowait(request)
        except asyncio.QueueFull:
            return False
        if request.name == "refresh":
            self._last_refresh = time.monotonic()
            self._refresh_pending = True
        self.changed()
        return True

    def forget(self, key: str) -> None:
        """Stop tracking a key, or every gone key when none is named."""
        if key:
            removed = 1 if self.entries.pop(key, None) is not None else 0
        else:
            gone = [k for k, e in self.entries.items() if e.status is Status.GONE]
            for k in gone:
                del self.entries[k]
            removed = len(gone)
        self.log(Kind.INFO, f"stopped tracking {removed} key(s)")

    async def run(self) -> None:
        """Pump pushes, run queued operations and schedule refreshes."""
        async with asyncio.TaskGroup() as group:
            group.create_task(self._pump())
            group.create_task(self._work())
            group.create_task(self._tick())

    def _lost(self, reason: str) -> None:
        """Report the first transport failure and reject further submissions."""
        if self.connected:
            self.connected = False
            self.log(Kind.ERROR, f"connection lost: {reason}")

    def _track(self, key: str) -> Entry:
        """Find or create a key preview, discarding a gone key first at capacity."""
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

    async def _pump(self) -> None:
        """Count and log publications until the connection closes."""
        while True:
            try:
                message = await self.client.next_message()
            except ConnectionError as exc:
                self._lost(str(exc))
                return
            self.stats.received += 1
            self.log(
                Kind.PUSH, f"{label(message.channel)}  {preview_text(message.payload)}"
            )

    async def _tick(self) -> None:
        """Refresh TTLs periodically and wake the display for countdowns."""
        while True:
            await asyncio.sleep(TICK_SECONDS)
            now = time.monotonic()
            age = now - self._last_refresh
            overdue = any(
                e.expires_at is not None and e.expires_at <= now
                for e in self.entries.values()
            )
            if (
                self.connected
                and self.entries
                and (age >= REFRESH_SECONDS or (overdue and age >= 1.0))
            ):
                self.submit(Request("refresh"))
            self.changed()

    async def _work(self) -> None:
        """Run queued requests in order and release bookkeeping on cancellation."""
        while True:
            request = await self._queue.get()
            try:
                if self.connected:
                    await self._perform(request)
            finally:
                if request.name == "refresh":
                    self._refresh_pending = False
                    self._last_refresh = time.monotonic()
                self._queue.task_done()

    async def _perform(self, request: Request) -> None:
        """Bound execution time, report expected failures and record statistics."""
        verb = request.name.upper()
        started = time.perf_counter()
        failed = True
        try:
            async with asyncio.timeout(OP_TIMEOUT_SECONDS):
                await self._execute(request)
        except TimeoutError:
            self.log(Kind.ERROR, f"{verb} timed out after {OP_TIMEOUT_SECONDS:g}s")
        except CommandError as exc:
            self.log(Kind.ERROR, f"{verb} refused: {exc}")
        except ProtocolError as exc:
            self.log(Kind.ERROR, f"{verb} rejected: {exc}")
        except OSError as exc:
            self._lost(str(exc))
        else:
            failed = False
        self.stats.record_operation(
            (time.perf_counter() - started) * 1000, failed=failed
        )
        self.changed()

    async def _execute(self, request: Request) -> None:
        """Dispatch a parsed network command to its operation handler."""
        handlers = {
            "get": self._get,
            "set": self._set,
            "setx": self._set,
            "del": self._delete,
            "expire": self._expire,
            "persist": self._persist,
            "ttl": self._ttl,
            "sub": self._subscription,
            "unsub": self._subscription,
            "pub": self._publish,
            "refresh": self._refresh,
        }
        await handlers[request.name](request)

    async def _set(self, request: Request) -> None:
        """Store bytes and replace any cached value and TTL."""
        key = request.target
        await self.client.set(key, request.data)
        entry = self._track(key)
        entry.set_value(request.data)
        entry.set_ttl(None, time.monotonic())
        self.log(Kind.OK, f"SET {label(key)} ({len(request.data)} bytes)")

    async def _delete(self, request: Request) -> None:
        """Delete a key and mark its local preview gone."""
        key = request.target
        removed = await self.client.delete(key)
        self._track(key).set_gone()
        outcome = "deleted" if removed else "not found"
        self.log(Kind.OK if removed else Kind.INFO, f"DEL {label(key)} → {outcome}")

    async def _expire(self, request: Request) -> None:
        """Set a TTL and update the preview without reading the value."""
        key = request.target
        found = await self.client.expire(key, request.ms)
        self._apply_ttl(self._track(key), request.ms if found else MISSING)
        text = format_duration(request.ms / 1000) if found else "not found"
        self.log(Kind.OK if found else Kind.INFO, f"EXPIRE {label(key)} → {text}")

    async def _persist(self, request: Request) -> None:
        """Remove a TTL, checking existence when the key was already persistent."""
        key = request.target
        if await self.client.persist(key):
            self._apply_ttl(self._track(key), NO_EXPIRY)
            self.log(Kind.OK, f"PERSIST {label(key)} → TTL removed")
        else:
            self._apply_ttl(self._track(key), await self.client.ttl(key))
            self.log(Kind.INFO, f"PERSIST {label(key)} → no TTL to remove")

    async def _ttl(self, request: Request) -> None:
        """Fetch and display a key's TTL without counting a cache read."""
        key = request.target
        ttl = await self.client.ttl(key)
        self._apply_ttl(self._track(key), ttl)
        text = {MISSING: "missing", NO_EXPIRY: "no expiry"}.get(
            ttl, format_duration(ttl / 1000)
        )
        self.log(Kind.OK, f"TTL {label(key)} → {text}")

    async def _subscription(self, request: Request) -> None:
        """Apply a subscription change after the server acknowledges it."""
        channel = request.target
        if request.name == "sub":
            await self.client.subscribe(channel)
            self.channels.add(channel)
            verb = "SUBSCRIBE"
        else:
            await self.client.unsubscribe(channel)
            self.channels.discard(channel)
            verb = "UNSUBSCRIBE"
        self.log(Kind.OK, f"{verb} {label(channel)}")

    async def _publish(self, request: Request) -> None:
        """Publish bytes and count the acknowledged publication."""
        count = await self.client.publish(request.target, request.data)
        self.stats.published += 1
        self.log(Kind.OK, f"PUBLISH {label(request.target)} → {count} subscriber(s)")

    async def _get(self, request: Request) -> None:
        """Fetch a value and TTL in one batch, counting only the GET result."""
        key = request.target
        value_reply, ttl_reply = await self.client.batch([Get(key), TTL(key)])
        entry = self._track(key)
        match value_reply:
            case Value(raw):
                self.stats.hits += 1
                entry.set_value(raw)
                self._apply_ttl(entry, _ttl_of(ttl_reply))
                self.log(Kind.OK, f"GET {label(key)} → {len(raw)} bytes")
            case Nil():
                self.stats.misses += 1
                entry.set_gone()
                self.log(Kind.INFO, f"GET {label(key)} → not found")
            case Err(message):
                raise CommandError(message)
            case _:
                raise ProtocolError(f"unexpected reply to GET: {value_reply!r}")

    def _apply_ttl(self, entry: Entry, ttl: int) -> None:
        """Apply a normalized TTL reply to a tracked key."""
        if ttl == MISSING:
            entry.set_gone()
        else:
            entry.set_ttl(None if ttl == NO_EXPIRY else ttl, time.monotonic())

    async def _refresh(self, request: Request) -> None:
        """Re-check every tracked key's existence and TTL with one batch."""
        keys = list(self.entries)
        if not keys:
            return
        replies = await self.client.batch([TTL(key) for key in keys])
        for key, reply in zip(keys, replies, strict=True):
            entry = self.entries.get(key)
            if entry is None:
                continue
            ttl = _ttl_of(reply)
            was_live = entry.status is Status.LIVE
            self._apply_ttl(entry, ttl)
            if was_live and entry.status is Status.GONE:
                self.log(Kind.INFO, f"gone: {label(key)} (expired, evicted or deleted)")
