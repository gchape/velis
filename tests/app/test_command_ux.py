"""Exercise the terminal's task-oriented vocabulary and command discovery."""

import curses
import unittest
from unittest.mock import Mock, patch

from velis.net.client import Client
from velis.tui.commands import (
    Request,
    UsageError,
    command_names,
    command_spec,
    parse_line,
)
from velis.tui.editor import CommandEditor
from velis.tui.models import Entry, Kind
from velis.tui.state import ViewState
from velis.tui.ui import Tui
from velis.tui.workspace import Workspace


class VocabularyTests(unittest.TestCase):
    """Keep command discovery and parsing consistent with application metadata."""

    def test_plain_language_actions_parse_to_canonical_requests(self) -> None:
        cases = {
            'write "two words" "hello world"': Request(
                "write", "two words", b"hello world"
            ),
            "read name": Request("read", "name"),
            "remove name": Request("remove", "name"),
            "lifetime name 5m": Request("lifetime", "name", ms=300_000),
            "remaining name": Request("remaining", "name"),
            "keep name": Request("keep", "name"),
            "watch updates": Request("watch", "updates"),
            "unwatch updates": Request("unwatch", "updates"),
            'send updates "hello"': Request("send", "updates", b"hello"),
            "sync": Request("sync"),
            "untrack name": Request("untrack", "name"),
            "status": Request("status"),
        }
        for line, expected in cases.items():
            with self.subTest(line=line):
                self.assertEqual(parse_line(line), expected)

    def test_old_commands_and_hex_write_are_rejected(self) -> None:
        for line in (
            "get name",
            "set name value",
            "setx name ff",
            "bytes name ff",
            "del name",
            "expire name 1s",
            "ttl name",
            "persist name",
            "sub events",
            "pub events hello",
            "refresh",
            "stats",
        ):
            with self.subTest(line=line), self.assertRaises(UsageError):
                parse_line(line)

    def test_navigation_aliases_and_usage_are_discoverable(self) -> None:
        self.assertEqual(parse_line("?"), Request("help"))
        self.assertEqual(parse_line("exit"), Request("quit"))
        self.assertEqual(command_spec("READ"), command_spec("read"))
        with self.assertRaisesRegex(UsageError, "write KEY TEXT"):
            parse_line("write name")

    def test_completion_only_lists_commands_allowed_in_current_mode(self) -> None:
        self.assertEqual(command_names("w"), ("watch", "write"))
        self.assertEqual(command_names("w", read_only=True), ("watch",))
        self.assertNotIn("write", command_names("", read_only=True))
        self.assertIn("remaining", command_names("", read_only=True))


class CompletionTests(unittest.TestCase):
    """Complete action words without altering arguments or submitting work."""

    def setUp(self) -> None:
        self.state = ViewState()
        self.editor = CommandEditor(self.state)

    def test_unique_match_adds_argument_space(self) -> None:
        self.editor.open("lif")
        self.editor.complete()
        self.assertEqual(self.state.line, "lifetime ")
        self.assertEqual(self.state.cursor, len(self.state.line))

    def test_ambiguous_match_keeps_common_prefix(self) -> None:
        self.editor.open("re")
        self.editor.complete()
        self.assertEqual(self.state.line, "re")

    def test_read_only_completion_skips_writes(self) -> None:
        self.editor.open("w")
        self.editor.complete(read_only=True)
        self.assertEqual(self.state.line, "watch ")

    def test_arguments_and_midword_edits_remain_untouched(self) -> None:
        for line, cursor in (("write 'two words'", 17), ("remaining", 3)):
            self.editor.open(line)
            self.state.cursor = cursor
            self.editor.complete()
            self.assertEqual(self.state.line, line)
            self.assertEqual(self.state.cursor, cursor)


class ActionShortcutTests(unittest.TestCase):
    """Route navigation and action shortcuts through shared application policy."""

    def setUp(self) -> None:
        self.screen = Mock(spec=curses.window)
        self.screen.getmaxyx.return_value = (24, 100)
        self.workspace = Workspace(Mock(spec=Client))
        self.workspace.model.entries["two words"] = Entry("two words")
        with patch("velis.tui.surface.curses.has_colors", return_value=False):
            self.tui = Tui(self.screen, self.workspace, host="localhost", port=9000)

    def test_action_shortcuts_use_quoted_keys_and_new_vocabulary(self) -> None:
        for key, word in (
            ("w", "write"),
            ("l", "lifetime"),
            ("x", "remove"),
            ("p", "keep"),
        ):
            self.tui.input._browse(key)
            self.assertEqual(self.tui._state.line, f"{word} 'two words' ")

    def test_tab_completes_without_submitting_a_request(self) -> None:
        self.tui.input._open_line("wat")
        self.tui.input._edit("\t")
        self.assertEqual(self.tui._state.line, "watch ")
        self.assertEqual(self.workspace.pending, 0)

    def test_read_only_admission_reports_the_application_rejection(self) -> None:
        self.workspace.read_only = True
        self.tui.input._submit(Request("write", "name", b"value"))
        self.assertEqual(self.workspace.pending, 0)
        self.assertEqual(self.workspace.activity[-1].kind, Kind.ERROR)
        self.assertIn("write is disabled", self.workspace.activity[-1].text)
