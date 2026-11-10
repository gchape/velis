"""Check local filtering without changing cache state or issuing network reads."""

import asyncio
import curses
import unittest
from unittest.mock import Mock

from velis.net.client import Client
from velis.tui.filtering import KeyFilter
from velis.tui.keyboard import KeyboardController
from velis.tui.models import Entry
from velis.tui.state import Mode, ViewState
from velis.tui.workspace import Workspace


class FilteringTests(unittest.TestCase):
    def test_query_preview_cancel_and_accept(self) -> None:
        state = ViewState(query="initial")
        editor = KeyFilter(state)
        editor.open()
        editor.handle("\x15")
        for char in "界":
            editor.handle(char)
        self.assertEqual(state.query, "界")
        editor.handle("\x1b")
        self.assertEqual(state.query, "initial")
        self.assertEqual(state.mode, Mode.BROWSE)
        editor.open()
        editor.handle(curses.KEY_BACKSPACE)
        editor.handle("\n")
        self.assertEqual(state.query, "initia")

    def test_filtered_selection_reads_only_selected_visible_key(self) -> None:
        client = Mock(spec=Client)
        workspace = Workspace(client)
        workspace.model.entries.update({key: Entry(key) for key in ("alpha", "BETA")})
        state = ViewState(query="beta")
        keyboard = KeyboardController(workspace, state, asyncio.Event())
        keyboard.handle("\n")
        self.assertEqual(workspace._queue.get_nowait().request.target, "BETA")
        self.assertEqual(state.query, "")
        self.assertEqual(state.follow, "BETA")
        client.read_entry.assert_not_awaited()

    def test_no_matches_does_not_read_and_escape_clears_filter(self) -> None:
        client = Mock(spec=Client)
        workspace = Workspace(client)
        state = ViewState(query="none")
        keyboard = KeyboardController(workspace, state, asyncio.Event())
        keyboard.handle("\n")
        self.assertEqual(workspace.pending, 0)
        keyboard.handle("\x1b")
        self.assertEqual(state.query, "")
