"""Command editing and scrollable informational screens without terminal I/O."""

import curses
import unittest

from velis.tui.editor import HISTORY_LINES, MAX_LINE, CommandEditor
from velis.tui.keyboard import KeyboardController
from velis.tui.state import Mode, ViewState


class CommandEditorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.state = ViewState()
        self.editor = CommandEditor(self.state)

    def test_history_restores_draft_and_keeps_edited_recall_separate(self) -> None:
        self.editor.open("read first")
        self.editor.accept()
        self.editor.open("unfinished")
        self.editor.handle(curses.KEY_UP)
        self.assertEqual(self.state.line, "read first")
        self.editor.handle(curses.KEY_DOWN)
        self.assertEqual(self.state.line, "unfinished")
        self.editor.handle(curses.KEY_UP)
        self.editor.handle("!")
        self.editor.handle(curses.KEY_UP)
        self.assertEqual(self.state.line, "read first")
        self.editor.handle(curses.KEY_DOWN)
        self.assertEqual(self.state.line, "read first!")

    def test_history_is_bounded_and_ignores_blank_or_duplicate_commands(self) -> None:
        for index in range(HISTORY_LINES + 3):
            self.editor.open(f"read key{index}")
            self.editor.accept()
            self.editor.accept()
        self.editor.open("  ")
        self.assertIsNone(self.editor.accept())
        for _ in range(HISTORY_LINES + 5):
            self.editor.handle(curses.KEY_UP)
        self.assertEqual(self.state.line, "read key3")

    def test_line_limit_still_allows_navigation_and_deletion(self) -> None:
        self.editor.open("a" * MAX_LINE)
        self.editor.handle("x")
        self.assertEqual(len(self.state.line), MAX_LINE)
        self.editor.handle(curses.KEY_HOME)
        self.editor.handle(curses.KEY_DC)
        self.editor.handle("x")
        self.assertEqual(self.state.line, "x" + "a" * (MAX_LINE - 1))
        self.assertEqual(self.state.cursor, 1)

    def test_unicode_editing_and_control_keys_preserve_cursor_bounds(self) -> None:
        self.editor.open("write 界 hello")
        self.editor.handle("\x17")
        self.assertEqual(self.state.line, "write 界 ")
        self.editor.handle(curses.KEY_LEFT)
        self.editor.handle("\x15")
        self.assertEqual(self.state.line, " ")
        self.assertEqual(self.state.cursor, 0)
        self.editor.handle(curses.KEY_BACKSPACE)
        self.editor.handle("\x00")
        self.assertEqual(self.state.line, " ")


class OverlayNavigationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tui = object.__new__(KeyboardController)
        self.tui._state = ViewState(mode=Mode.HELP, overlay_total=35, overlay_page=10)

    def test_scroll_keys_keep_overlay_open_and_clamp_to_content(self) -> None:
        for key, offset in (
            (curses.KEY_NPAGE, 10),
            ("j", 11),
            (curses.KEY_END, 25),
            (curses.KEY_DOWN, 25),
            (curses.KEY_PPAGE, 15),
            ("g", 0),
            ("k", 0),
        ):
            with self.subTest(key=key):
                self.tui.handle(key)
                self.assertEqual(self.tui._state.overlay_scroll, offset)
                self.assertEqual(self.tui._state.mode, Mode.HELP)
        self.tui.handle("\x1b")
        self.assertEqual(self.tui._state.mode, Mode.BROWSE)

    def test_opening_another_overlay_resets_position(self) -> None:
        self.tui._state.overlay_scroll = 20
        self.tui._open_overlay(Mode.STATS)
        self.assertEqual(self.tui._state.overlay_scroll, 0)
        self.assertEqual(self.tui._state.mode, Mode.STATS)
