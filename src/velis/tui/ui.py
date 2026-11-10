"""Translate terminal input into local actions and queued client commands."""

import asyncio
import curses
import signal
import sys
from contextlib import ExitStack

from .keyboard import KeyboardController
from .state import ViewState
from .view import Renderer
from .workspace import Workspace

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
        self.input = KeyboardController(workspace, self._state, self._stop)
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
                self.input.handle(key)
                handled = True
        except Exception as exc:
            self._failure = exc
            self._stop.set()
        if handled:
            self._wake.set()
