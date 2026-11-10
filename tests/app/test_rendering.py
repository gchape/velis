"""Check readable content and complete navigation at supported terminal sizes."""

import curses
import unittest
from unittest.mock import Mock, patch

from velis.net.client import Client
from velis.tui.chrome import command_hint
from velis.tui.layout import Layout
from velis.tui.models import Entry
from velis.tui.state import Mode
from velis.tui.surface import Theme
from velis.tui.text import cells, describe_value, wrap_cells
from velis.tui.ui import Tui
from velis.tui.workspace import Workspace


class RenderingTests(unittest.TestCase):
    """Exercise composed presenters through their visible output."""

    def setUp(self) -> None:
        self.screen = Mock(spec=curses.window)
        self.screen.getmaxyx.return_value = (14, 60)
        self.workspace = Workspace(Mock(spec=Client))
        with patch("velis.tui.surface.curses.has_colors", return_value=False):
            self.tui = Tui(self.screen, self.workspace, host="localhost", port=9000)

    def output(self) -> str:
        """Collect one frame's text without relying on specific draw call counts."""
        self.screen.addstr.reset_mock()
        self.tui._renderer.draw()
        return "\n".join(call.args[2] for call in self.screen.addstr.call_args_list)

    def test_small_screen_retains_value_and_activity_space(self) -> None:
        entry = self.workspace.model.entries["name"] = Entry("name")
        entry.set_value(b"a readable value")
        entry.set_ttl(None, 0)
        output = self.output()
        self.assertIn("a readable value", output)
        self.assertIn("Activity", output)
        layout = Layout.for_size(14, 60)
        self.assertLess(layout.value.y + 3, layout.activity.y)
        self.assertEqual(layout.activity.y + layout.activity.rows, 12)

    def test_help_can_reach_last_line_and_resets_on_reopening(self) -> None:
        self.tui.input.handle("?")
        self.output()
        self.tui.input.handle(curses.KEY_END)
        self.assertIn("loopback or a tunnel", self.output().replace("\n", ""))
        self.assertGreater(self.tui._state.overlay_scroll, 0)
        self.tui.input.handle("\x1b")
        self.tui.input.handle("?")
        self.assertIn("Start here", self.output())
        self.assertEqual(self.tui._state.overlay_scroll, 0)

    def test_read_only_empty_state_offers_reads(self) -> None:
        self.workspace.read_only = True
        output = self.output()
        self.assertIn(":read name", output)
        self.assertNotIn(":write", output)
        self.assertIn("READ-ONLY", output)

    def test_command_mode_shows_usage_and_cursor(self) -> None:
        self.tui.input._open_line("lifetime ")
        self.assertIn("lifetime KEY TIME", self.output())
        self.screen.move.assert_called()
        self.assertEqual(self.tui._state.mode, Mode.COMMAND)
        self.assertIn("disabled", command_hint("write", True))

    def test_no_color_disables_all_color_initialization(self) -> None:
        with (
            patch.dict("os.environ", {"NO_COLOR": "1"}),
            patch("velis.tui.surface.curses.has_colors", return_value=True),
            patch("velis.tui.surface.curses.init_pair") as init_pair,
        ):
            theme = Theme()
        init_pair.assert_not_called()
        self.assertTrue(theme.selected)


class PreviewTests(unittest.TestCase):
    """Keep narrow and Unicode previews complete and bounded by terminal cells."""

    def test_wrapping_preserves_wide_characters_and_combining_marks(self) -> None:
        text = "a界e\u0301" * 30
        lines = wrap_cells(text, 17)
        self.assertEqual("".join(lines), text)
        self.assertTrue(all(cells(line) <= 17 for line in lines))

    def test_text_preview_wraps_instead_of_discarding_long_line_tails(self) -> None:
        raw = b"hello world " * 20
        lines = describe_value(raw, len(raw), 25)
        self.assertEqual("".join(lines), raw.decode())
        self.assertTrue(all(cells(line) <= 25 for line in lines))

    def test_binary_preview_fits_narrow_value_pane(self) -> None:
        raw = bytes(range(64))
        lines = describe_value(raw, len(raw), 25)
        self.assertTrue(all(cells(line) <= 25 for line in lines))
        self.assertIn("00003f", lines[-1])
