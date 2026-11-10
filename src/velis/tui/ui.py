"""Translate terminal input into local actions and queued client commands."""

import asyncio
import curses
import shlex
import signal
import sys
from collections import deque
from contextlib import ExitStack

from .commands import KEY_COMMANDS, SPECS, Request, UsageError, parse_line
from .models import Kind
from .view import Mode, Renderer, ViewState
from .workspace import Workspace

HISTORY_LINES = 100
MAX_LINE = 8192
FRAME_SECONDS = 0.03
ESCAPE_SECONDS = 0.04


class Tui:
    """Drive terminal input and coordinate rendering with workspace tasks."""

    def __init__(
        self, scr: curses.window, workspace: Workspace, *, host: str, port: int
    ) -> None:
        """Create one input controller and renderer for the connected workspace."""
        self._scr = scr
        self._ws = workspace
        self._state = ViewState()
        self._renderer = Renderer(scr, workspace, self._state, host=host, port=port)
        self._wake = asyncio.Event()
        self._stop = asyncio.Event()
        self._failure: Exception | None = None
        self._history: deque[str] = deque(maxlen=HISTORY_LINES)
        self._history_pos: int | None = None
        self._draft = ""
        workspace.changed = self._wake.set

    async def run(self) -> None:
        """Run input and background tasks, restoring handlers on every exit."""
        loop = asyncio.get_running_loop()
        fd = sys.stdin.fileno()
        with ExitStack() as cleanup:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, self._stop.set)
                cleanup.callback(loop.remove_signal_handler, sig)
            loop.add_reader(fd, self._on_readable)
            cleanup.callback(loop.remove_reader, fd)
            tasks = [
                asyncio.create_task(self._ws.run()),
                asyncio.create_task(self._render()),
            ]
            stopper = asyncio.create_task(self._stop.wait())
            try:
                await asyncio.wait(
                    [*tasks, stopper], return_when=asyncio.FIRST_COMPLETED
                )
            finally:
                for task in (*tasks, stopper):
                    task.cancel()
                results = await asyncio.gather(*tasks, return_exceptions=True)
                await asyncio.gather(stopper, return_exceptions=True)
        if self._failure is not None:
            raise self._failure
        for result in results:
            if isinstance(result, Exception):
                raise result

    async def _render(self) -> None:
        """Redraw on changes, with a frame limit during bursts of activity."""
        self._wake.set()
        while True:
            await self._wake.wait()
            self._wake.clear()
            self._poll()
            self._renderer.draw()
            await asyncio.sleep(FRAME_SECONDS)

    def _on_readable(self) -> None:
        """Read input, then look again shortly after.

        curses holds a lone Esc back in case it starts an escape sequence, and
        no further bytes arrive to wake the reader, so poll once more after the
        escape delay has passed.
        """
        self._poll()
        asyncio.get_running_loop().call_later(ESCAPE_SECONDS, self._poll)

    def _poll(self) -> None:
        """Handle all pending keys; also notices terminal resizes."""
        handled = False
        try:
            while True:
                try:
                    key = self._scr.get_wch()
                except curses.error:
                    break
                self._handle(key)
                handled = True
        except Exception as exc:
            self._failure = exc
            self._stop.set()
        if handled:
            self._wake.set()

    def _handle(self, key: str | int) -> None:
        """Dispatch a key according to the current interface mode."""
        if key == curses.KEY_RESIZE:
            curses.update_lines_cols()
            self._scr.clear()
        elif self._state.mode in {Mode.HELP, Mode.STATS}:
            self._state.mode = Mode.BROWSE
        elif self._state.mode is Mode.COMMAND:
            self._edit(key)
        else:
            self._browse(key)

    def _browse(self, key: str | int) -> None:
        """Run browse shortcuts after handling navigation."""
        if self._navigate(key):
            return
        match key:
            case "q":
                self._stop.set()
            case "\n" | "\r" | curses.KEY_ENTER:
                self._get_selected()
            case ":":
                self._open_line("")
            case "s" | "e" | "d" | "p":
                verb = {"s": "set", "e": "expire", "d": "del", "p": "persist"}[str(key)]
                self._prefill(verb)
            case "r":
                self._submit(Request("refresh"))
            case "c":
                self._ws.activity.clear()
            case "?":
                self._state.mode = Mode.HELP
            case "t":
                self._state.mode = Mode.STATS
            case _:
                pass

    def _get_selected(self) -> None:
        """Fetch the selected key when the workspace has any tracked keys."""
        if (entry := self._state.selected_entry(self._ws)) is not None:
            self._submit(Request("get", entry.key))

    def _navigate(self, key: str | int) -> bool:
        """Move the key selection or value scroll; report whether key matched."""
        state = self._state
        last = max(0, len(self._ws.entries) - 1)
        match key:
            case curses.KEY_UP | "k":
                state.selected = max(0, state.selected - 1)
            case curses.KEY_DOWN | "j":
                state.selected = min(last, state.selected + 1)
            case curses.KEY_HOME | "g":
                state.selected = 0
            case curses.KEY_END | "G":
                state.selected = last
            case curses.KEY_PPAGE:
                state.scroll = max(0, state.scroll - state.page)
            case curses.KEY_NPAGE:
                state.scroll += state.page
            case _:
                return False
        return True

    def _prefill(self, verb: str) -> None:
        """Start a write command with the selected key quoted as one argument."""
        if self._ws.read_only:
            self._ws.log(Kind.ERROR, f"read-only mode: {verb} is disabled")
            return
        entry = self._state.selected_entry(self._ws)
        self._open_line(f"{verb} {shlex.quote(entry.key)} " if entry else f"{verb} ")

    def _open_line(self, text: str) -> None:
        """Open command mode with the cursor at the end of the supplied draft."""
        self._state.mode = Mode.COMMAND
        self._state.line, self._state.cursor = text, len(text)
        self._history_pos = None

    def _edit(self, key: str | int) -> None:
        """Handle command submission, cancellation and history navigation."""
        match key:
            case "\x1b":
                self._state.mode = Mode.BROWSE
            case "\n" | "\r" | curses.KEY_ENTER:
                self._accept_line()
            case curses.KEY_UP | curses.KEY_DOWN:
                self._browse_history(-1 if key == curses.KEY_UP else 1)
            case _:
                self._edit_line(key)
                self._history_pos = None

    def _accept_line(self) -> None:
        """Remember a nonempty command and run it after leaving command mode."""
        line = self._state.line
        self._state.mode = Mode.BROWSE
        if line.strip():
            if not self._history or self._history[-1] != line:
                self._history.append(line)
            self._run_line(line)

    def _edit_line(self, key: str | int) -> None:
        """Apply a cursor movement, deletion or printable character insertion."""
        if self._move_cursor(key):
            return
        state = self._state
        line, cur = state.line, state.cursor
        match key:
            case curses.KEY_BACKSPACE | "\x7f" | "\x08":
                before = max(0, cur - 1)
                state.line, state.cursor = line[:before] + line[cur:], before
            case curses.KEY_DC:
                state.line = line[:cur] + line[cur + 1 :]
            case "\x15":
                state.line, state.cursor = line[cur:], 0
            case "\x17":
                head = line[:cur].rstrip()
                head = head[: head.rfind(" ") + 1]
                state.line, state.cursor = head + line[cur:], len(head)
            case str() if key.isprintable() and len(line) < MAX_LINE:
                state.line, state.cursor = line[:cur] + key + line[cur:], cur + 1
            case _:
                pass

    def _move_cursor(self, key: str | int) -> bool:
        """Handle command cursor navigation and report whether a key matched."""
        state = self._state
        match key:
            case curses.KEY_LEFT:
                state.cursor = max(0, state.cursor - 1)
            case curses.KEY_RIGHT:
                state.cursor = min(len(state.line), state.cursor + 1)
            case curses.KEY_HOME | "\x01":
                state.cursor = 0
            case curses.KEY_END | "\x05":
                state.cursor = len(state.line)
            case _:
                return False
        return True

    def _browse_history(self, step: int) -> None:
        """Browse remembered commands while preserving the current draft."""
        if not self._history:
            return
        if self._history_pos is None:
            if step > 0:
                return
            self._draft = self._state.line
            self._history_pos = len(self._history) - 1
        else:
            self._history_pos += step
            self._history_pos = max(0, self._history_pos)
        if self._history_pos >= len(self._history):
            self._history_pos = None
            self._state.line = self._draft
        else:
            self._state.line = self._history[self._history_pos]
        self._state.cursor = len(self._state.line)

    def _run_line(self, line: str) -> None:
        """Parse a command, run local actions or submit a network operation."""
        try:
            request = parse_line(line)
        except UsageError as exc:
            self._ws.log(Kind.ERROR, str(exc))
            return
        match request.name:
            case "quit":
                self._stop.set()
            case "help":
                self._state.mode = Mode.HELP
            case "stats":
                self._state.mode = Mode.STATS
            case "clear":
                self._ws.activity.clear()
            case "forget":
                self._ws.forget(request.target)
            case _:
                self._submit(request)

    def _submit(self, request: Request) -> None:
        """Enforce read-only mode and queue capacity before following a key."""
        ws = self._ws
        if SPECS[request.name].mutates and ws.read_only:
            ws.log(Kind.ERROR, f"read-only mode: {request.name} is disabled")
        elif not ws.connected:
            ws.log(Kind.ERROR, "not connected")
        elif not ws.submit(request):
            ws.log(Kind.WARN, "too many pending operations; wait a moment")
        elif request.name in KEY_COMMANDS:
            self._state.follow = request.target
