"""Route keyboard actions to editing, local presentation or application submission."""

import asyncio
import curses
import shlex

from .admission import Submission
from .commands import KEY_COMMANDS, Request, UsageError, parse_line
from .editor import CommandEditor
from .filtering import KeyFilter
from .models import Kind
from .state import Mode, ViewState
from .workspace import Workspace


class KeyboardController:
    """Own keyboard modes and delegate command admission to the workspace."""

    def __init__(
        self, workspace: Workspace, state: ViewState, stop: asyncio.Event
    ) -> None:
        self._ws = workspace
        self._state = state
        self._stop = stop
        self._editor = CommandEditor(state)
        self._filter = KeyFilter(state)

    def handle(self, key: str | int) -> None:
        """Dispatch a key according to the current interface mode."""
        if key == curses.KEY_RESIZE:
            curses.update_lines_cols()
        elif self._state.mode in {Mode.HELP, Mode.STATS}:
            self._overlay(key)
        elif self._state.mode is Mode.FILTER:
            self._filter.handle(key)
        elif self._state.mode is Mode.COMMAND:
            self._edit(key)
        else:
            self._browse(key)

    def _open_overlay(self, mode: Mode) -> None:
        """Open help or statistics at the start of its scrollable content."""
        self._state.mode = mode
        self._state.overlay_scroll = 0

    def _overlay(self, key: str | int) -> None:
        """Scroll informational screens or return to browsing on other keys."""
        state = self._state
        last = max(0, state.overlay_total - state.overlay_page)
        match key:
            case curses.KEY_UP | "k":
                state.overlay_scroll -= 1
            case curses.KEY_DOWN | "j":
                state.overlay_scroll += 1
            case curses.KEY_PPAGE:
                state.overlay_scroll -= state.overlay_page
            case curses.KEY_NPAGE:
                state.overlay_scroll += state.overlay_page
            case curses.KEY_HOME | "g":
                state.overlay_scroll = 0
            case curses.KEY_END | "G":
                state.overlay_scroll = last
            case _:
                state.mode = Mode.BROWSE
        state.overlay_scroll = max(0, min(last, state.overlay_scroll))

    def _browse(self, key: str | int) -> None:
        """Run browse shortcuts after handling navigation."""
        if self._navigate(key) or self._open_panel(key):
            return
        match key:
            case "q":
                self._stop.set()
            case "\n" | "\r" | curses.KEY_ENTER:
                self._get_selected()
            case "w" | "l" | "x" | "p":
                verb = {"w": "write", "l": "lifetime", "x": "remove", "p": "keep"}[
                    str(key)
                ]
                self._prefill(verb)
            case "r":
                self._submit(Request("sync"))
            case "c":
                self._ws.activity.clear()
            case _:
                pass

    def _open_panel(self, key: str | int) -> bool:
        """Open a local tool or clear the active filter without network work."""
        match key:
            case "/":
                self._filter.open()
            case "\x1b":
                self._state.query = ""
            case ":":
                self._open_line("")
            case "b":
                self._open_line("list ")
            case "?":
                self._open_overlay(Mode.HELP)
            case "t":
                self._open_overlay(Mode.STATS)
            case _:
                return False
        return True

    def _get_selected(self) -> None:
        """Fetch the selected key when the workspace has any tracked keys."""
        if (
            entry := self._state.selected_entry(list(self._ws.model.entries.values()))
        ) is not None:
            self._submit(Request("read", entry.key))

    def _navigate(self, key: str | int) -> bool:
        """Move the key selection or value scroll; report whether key matched."""
        state = self._state
        entries = self._state.visible_entries(list(self._ws.model.entries.values()))
        last = max(0, len(entries) - 1)
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
        entry = self._state.selected_entry(list(self._ws.model.entries.values()))
        self._open_line(f"{verb} {shlex.quote(entry.key)} " if entry else f"{verb} ")

    def _open_line(self, text: str) -> None:
        """Open command mode with the cursor at the end of the supplied draft."""
        self._state.mode = Mode.COMMAND
        self._editor.open(text)

    def _edit(self, key: str | int) -> None:
        """Handle command submission, cancellation and history navigation."""
        match key:
            case "\x1b":
                self._state.mode = Mode.BROWSE
            case "\n" | "\r" | curses.KEY_ENTER:
                self._accept_line()
            case "\t":
                self._editor.complete(read_only=self._ws.read_only)
            case _:
                self._editor.handle(key)

    def _accept_line(self) -> None:
        """Remember a nonempty command and run it after leaving command mode."""
        self._state.mode = Mode.BROWSE
        if (line := self._editor.accept()) is not None:
            self._run_line(line)

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
                self._open_overlay(Mode.HELP)
            case "status":
                self._open_overlay(Mode.STATS)
            case "clear":
                self._ws.activity.clear()
            case "untrack":
                self._ws.forget(request.target)
            case _:
                self._submit(request)

    def _submit(self, request: Request) -> None:
        """Present the application's admission result and follow accepted keys."""
        result = self._ws.submit(request)
        if result is Submission.ACCEPTED:
            if request.name in KEY_COMMANDS:
                self._state.query = ""
                self._state.follow = request.target
            return
        messages = {
            Submission.DISCONNECTED: "not connected",
            Submission.FULL: "too many pending operations; wait a moment",
            Submission.READ_ONLY: f"read-only mode: {request.name} is disabled",
            Submission.INVALID: "unknown or local command cannot be queued",
        }
        kind = Kind.WARN if result is Submission.FULL else Kind.ERROR
        self._ws.log(kind, messages[result])
