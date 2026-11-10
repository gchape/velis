"""Serialize client operations and maintain tracked keys and session activity."""

import asyncio
import time
from collections import deque
from collections.abc import Callable

from velis.errors import CommandError, ProtocolError
from velis.net.client import Client

from .admission import QueuedRequest, Submission, admission
from .commands import SPECS, Request
from .models import Activity, Kind
from .operations import Operations
from .session_model import SessionModel
from .text import clean, label, preview_text

ACTIVITY_LINES = 500
QUEUE_SIZE = 32
OP_TIMEOUT_SECONDS = 10.0
REFRESH_SECONDS = 5.0
TICK_SECONDS = 0.5


class Workspace:
    """Own the client, the tracked keys and the activity log; no curses here.

    Operations run strictly in submission order through one bounded queue, so
    a reply is never applied out of order and a stalled server cannot make the
    client pile up work. ``changed`` is called whenever there is something new
    to draw. Network command handlers are delegated to Operations; this class
    owns queueing, background tasks, timing and error reporting.
    """

    def __init__(
        self,
        client: Client,
        *,
        read_only: bool = False,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        """Initialize bounded session state around an already connected client."""
        self.client = client
        self.read_only = read_only
        self.model = SessionModel()
        self.activity: deque[Activity] = deque(maxlen=ACTIVITY_LINES)
        self.connected = True
        self.changed: Callable[[], None] = lambda: None
        self._queue: asyncio.Queue[QueuedRequest] = asyncio.Queue(QUEUE_SIZE)
        self._monotonic = monotonic
        self._last_refresh = time.monotonic()
        self._refresh_pending = False
        self._operations = Operations(client, self.model, self.log)

    @property
    def pending(self) -> int:
        """Return queued operations, excluding the operation currently running."""
        return self._queue.qsize()

    def log(self, kind: Kind, text: str) -> None:
        """Append a bounded, escaped activity message and request a redraw."""
        stamp = time.strftime("%H:%M:%S")
        self.activity.append(Activity(time.monotonic(), stamp, kind, clean(text[:300])))
        self.changed()

    def submit(self, request: Request) -> Submission:
        """Admit work with a deadline covering queue wait and network execution."""
        outcome = admission(request, connected=self.connected, read_only=self.read_only)
        if not outcome:
            return outcome
        if request.name == "sync" and self._refresh_pending:
            return Submission.ACCEPTED
        queued = QueuedRequest(request, self._monotonic() + OP_TIMEOUT_SECONDS)
        try:
            self._queue.put_nowait(queued)
        except asyncio.QueueFull:
            return Submission.FULL
        if request.name == "sync":
            self._last_refresh = time.monotonic()
            self._refresh_pending = True
        self.changed()
        return Submission.ACCEPTED

    def forget(self, key: str) -> None:
        """Stop tracking a key, or every gone key when none is named."""
        removed = self.model.forget(key)
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

    async def _pump(self) -> None:
        """Count and log publications until the connection closes."""
        while True:
            try:
                message = await self.client.next_message()
            except ConnectionError as exc:
                self._lost(str(exc))
                return
            self.model.stats.received += 1
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
                for e in self.model.entries.values()
            )
            if (
                self.connected
                and self.model.entries
                and (age >= REFRESH_SECONDS or (overdue and age >= 1.0))
            ):
                self.submit(Request("sync"))
            self.changed()

    async def _work(self) -> None:
        """Run queued requests in order and release bookkeeping on cancellation."""
        while True:
            queued = await self._queue.get()
            request = queued.request
            try:
                await self._dispatch(queued)
            finally:
                if request.name == "sync":
                    self._refresh_pending = False
                    self._last_refresh = time.monotonic()
                self._queue.task_done()

    async def _dispatch(self, queued: QueuedRequest) -> None:
        """Recheck mutable policy and discard stale work before sending it."""
        request = queued.request
        outcome = admission(request, connected=self.connected, read_only=self.read_only)
        if not outcome:
            self.log(
                Kind.ERROR,
                f"{request.name}: {outcome.name.lower().replace('_', ' ')}; not sent",
            )
            return
        remaining = queued.deadline - self._monotonic()
        if remaining <= 0:
            self.log(
                Kind.WARN, f"{SPECS[request.name].name} expired in queue; not sent"
            )
            return
        await self._perform(request, remaining)

    async def _perform(
        self, request: Request, remaining: float = OP_TIMEOUT_SECONDS
    ) -> None:
        """Bound remaining execution time and record completed attempts only."""
        verb = SPECS[request.name].name
        started = time.perf_counter()
        failed = True
        try:
            async with asyncio.timeout(remaining):
                await self._operations.execute(request)
        except TimeoutError:
            self.log(
                Kind.ERROR, f"{verb} timed out; a sent operation may still complete"
            )
        except CommandError as exc:
            self.log(Kind.ERROR, f"{verb} refused: {exc}")
        except ProtocolError as exc:
            self.log(Kind.ERROR, f"{verb} rejected: {exc}")
        except OSError as exc:
            self._lost(str(exc))
        else:
            failed = False
        self.model.stats.record_operation(
            (time.perf_counter() - started) * 1000, failed=failed
        )
        self.changed()
