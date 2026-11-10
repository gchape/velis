"""Edit a local key filter without submitting commands or fetching values."""

import curses

from .state import Mode, ViewState

MAX_FILTER = 128


class KeyFilter:
    """Keep incremental search reversible until Enter accepts the query."""

    def __init__(self, state: ViewState) -> None:
        self._state = state
        self._original = ""

    def open(self) -> None:
        """Edit the current query and remember it for cancellation."""
        self._original = self._state.query
        self._state.mode = Mode.FILTER

    def handle(self, key: str | int) -> None:
        """Apply a query edit; Escape restores and Enter keeps the filter."""
        state = self._state
        match key:
            case "\x1b":
                state.query = self._original
                state.mode = Mode.BROWSE
            case "\n" | "\r" | curses.KEY_ENTER:
                state.mode = Mode.BROWSE
            case curses.KEY_BACKSPACE | "\x7f" | "\x08":
                state.query = state.query[:-1]
            case "\x15":
                state.query = ""
            case str() if key.isprintable() and len(state.query) < MAX_FILTER:
                state.query += key
            case _:
                return
        state.selected = state.top = 0
