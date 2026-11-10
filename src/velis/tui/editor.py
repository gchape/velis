"""Edit command drafts and browse bounded history independently of terminal I/O."""

import curses
from collections import deque

from .commands import command_names
from .state import ViewState

HISTORY_LINES = 100
MAX_LINE = 8192


class CommandEditor:
    """Own command history while editing the draft exposed to the renderer."""

    def __init__(self, state: ViewState) -> None:
        self._state = state
        self._history: deque[str] = deque(maxlen=HISTORY_LINES)
        self._history_pos: int | None = None
        self._draft = ""

    def open(self, text: str) -> None:
        """Start a draft and detach it from any recalled history entry."""
        self._state.line, self._state.cursor = text, len(text)
        self._history_pos = None

    def accept(self) -> str | None:
        """Remember nonempty commands once and return the accepted draft."""
        line = self._state.line
        if not line.strip():
            return None
        if not self._history or self._history[-1] != line:
            self._history.append(line)
        return line

    def complete(self, *, read_only: bool = False) -> None:
        """Complete only a command word, preserving quoted arguments and history."""
        state = self._state
        if state.cursor != len(state.line) or any(ch.isspace() for ch in state.line):
            return
        matches = command_names(state.line, read_only=read_only)
        if not matches:
            return
        word = matches[0]
        while not all(match.startswith(word) for match in matches):
            word = word[:-1]
        if len(matches) == 1:
            word += " "
        state.line, state.cursor = word, len(word)
        self._history_pos = None

    def handle(self, key: str | int) -> None:
        """Apply history navigation or a local edit without submitting commands."""
        if key in (curses.KEY_UP, curses.KEY_DOWN):
            self._browse_history(-1 if key == curses.KEY_UP else 1)
        else:
            self._edit_line(key)
            self._history_pos = None

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
