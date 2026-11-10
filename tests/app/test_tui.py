"""Exercise terminal behavior, session counters and serialized operations."""

import asyncio
import curses
import unittest
from contextlib import suppress
from unittest.mock import Mock, patch

from velis.core.messages import (
    NO_EXPIRY,
    TTL,
    Err,
    Get,
    Message,
    Nil,
    Persistent,
    Value,
)
from velis.errors import CommandError, ProtocolError
from velis.net.client import Client
from velis.tui.commands import Request, UsageError, parse_line
from velis.tui.models import Entry, Kind, Status
from velis.tui.text import cells, clean, command_window
from velis.tui.ui import Tui
from velis.tui.view import Mode
from velis.tui.workspace import Workspace


class CommandTests(unittest.TestCase):
    """Keep typed commands and usage errors stable across the package split."""

    def test_quoted_values_aliases_durations_and_local_stats(self) -> None:
        """Parse binary, quoted and local commands without network access."""
        cases = {
            'set "two words" "hello world"': Request(
                "set", "two words", b"hello world"
            ),
            'setx key "00 ff"': Request("setx", "key", b"\x00\xff"),
            "expire key 2h": Request("expire", "key", ms=7_200_000),
            "DELETE key": Request("del", "key"),
            "stats": Request("stats"),
        }
        for line, expected in cases.items():
            with self.subTest(line=line):
                self.assertEqual(parse_line(line), expected)

    def test_bad_commands_report_usage_errors(self) -> None:
        """Reject malformed input before it can enter the request queue."""
        for line in (
            "",
            'set key "unfinished',
            "get",
            "stats extra",
            "setx key zz",
            "expire key -1s",
            "expire key 999999999999999999d",
            "unknown",
        ):
            with self.subTest(line=line), self.assertRaises(UsageError):
                parse_line(line)


class WorkspaceTests(unittest.IsolatedAsyncioTestCase):
    """Check queued operations and statistics independently of curses."""

    def setUp(self) -> None:
        """Provide a typed client double with asynchronous command methods."""
        self.client = Mock(spec=Client)
        self.workspace = Workspace(self.client)

    async def run_commands(self, *lines: str) -> None:
        """Drain submitted commands and always cancel the waiting worker."""
        worker = asyncio.create_task(self.workspace._work())
        try:
            for line in lines:
                self.assertTrue(self.workspace.submit(parse_line(line)))
            async with asyncio.timeout(2):
                await self.workspace._queue.join()
        finally:
            worker.cancel()
            with suppress(asyncio.CancelledError):
                await worker

    async def test_commands_update_state_and_count_only_get_replies_as_reads(
        self,
    ) -> None:
        """Exercise every network handler, including binary values and refreshes."""
        self.client.batch.side_effect = [
            [Value(b"value"), Persistent()],
            [Nil(), Nil()],
            [Persistent(), Nil()],
        ]
        self.client.ttl.return_value = NO_EXPIRY
        self.client.expire.return_value = True
        self.client.persist.return_value = False
        self.client.delete.return_value = True
        self.client.publish.return_value = 2
        await self.run_commands(
            "set key value",
            "setx key 76616c7565",
            "get key",
            "get missing",
            "expire key 1s",
            "ttl key",
            "persist key",
            "sub updates",
            "pub updates hello",
            "unsub updates",
            "refresh",
            "del key",
        )
        stats = self.workspace.stats
        self.assertEqual((stats.operations, stats.errors), (12, 0))
        self.assertEqual((stats.hits, stats.misses, stats.hit_rate), (1, 1, 50))
        self.assertEqual(stats.published, 1)
        self.assertEqual(self.workspace.channels, set())
        self.assertEqual(self.workspace.entries["key"].status, Status.GONE)
        self.client.get.assert_not_awaited()
        self.assertEqual(
            self.client.batch.call_args_list[-1].args[0],
            [TTL("key"), TTL("missing")],
        )
        self.client.batch.assert_any_await([Get("key"), TTL("key")])

    async def test_failures_count_and_latency_includes_failed_attempts(self) -> None:
        """A rejection does not block later commands or disappear from the mean."""
        self.client.set.side_effect = [CommandError("denied\x1b"), None]
        with patch(
            "velis.tui.workspace.time.perf_counter", side_effect=[1, 1.02, 2, 2.06]
        ):
            await self.run_commands("set bad value", "set good value")
        stats = self.workspace.stats
        self.assertEqual((stats.operations, stats.errors), (2, 1))
        assert stats.last_ms is not None
        average_ms = stats.average_ms
        assert average_ms is not None
        self.assertAlmostEqual(stats.last_ms, 60)
        self.assertAlmostEqual(average_ms, 40)
        self.assertIsNone(stats.hit_rate)
        self.assertNotIn("bad", self.workspace.entries)
        self.assertEqual(self.workspace.entries["good"].preview, b"value")
        self.assertTrue(
            all(item.text.isprintable() for item in self.workspace.activity)
        )

    async def test_refresh_errors_are_reported_without_counting_reads(self) -> None:
        """A rejected TTL refresh increments errors and permits another refresh."""
        self.workspace.entries["key"] = Entry("key")
        self.client.batch.return_value = [Err("refresh rejected")]
        await self.run_commands("refresh")
        stats = self.workspace.stats
        self.assertEqual((stats.operations, stats.errors), (1, 1))
        self.assertEqual((stats.hits, stats.misses), (0, 0))
        self.assertIn("refresh rejected", self.workspace.activity[-1].text)
        self.assertTrue(self.workspace.submit(Request("refresh")))
        self.assertEqual(self.workspace.pending, 1)

    async def test_refreshes_coalesce_while_queued_and_running(self) -> None:
        """Slow background checks cannot accumulate duplicate refresh requests."""
        self.workspace.entries["key"] = Entry("key")
        started, release = asyncio.Event(), asyncio.Event()

        async def batch(commands: object) -> list[Persistent]:
            """Hold a refresh in flight until the test releases its reply."""
            started.set()
            await release.wait()
            return [Persistent()]

        self.client.batch.side_effect = batch
        self.workspace.submit(Request("refresh"))
        self.workspace.submit(Request("refresh"))
        self.assertEqual(self.workspace.pending, 1)
        worker = asyncio.create_task(self.workspace._work())
        try:
            async with asyncio.timeout(2):
                await started.wait()
                self.assertTrue(self.workspace.submit(Request("refresh")))
                self.assertEqual(self.workspace.pending, 0)
                release.set()
                await self.workspace._queue.join()
            self.client.batch.assert_awaited_once()
            self.assertEqual(self.workspace.stats.operations, 1)
        finally:
            worker.cancel()
            with suppress(asyncio.CancelledError):
                await worker

    async def test_transport_protocol_and_timeout_failures_are_counted(self) -> None:
        """Record expected failures while distinguishing disconnection."""
        for error in (
            ProtocolError("bad reply"),
            TimeoutError(),
            ConnectionError("lost"),
        ):
            with self.subTest(error=type(error).__name__):
                self.workspace = Workspace(self.client)
                self.client.set.side_effect = error
                await self.run_commands("set key value")
                self.assertEqual(self.workspace.stats.operations, 1)
                self.assertEqual(self.workspace.stats.errors, 1)
                self.assertEqual(
                    self.workspace.connected, not isinstance(error, ConnectionError)
                )

    async def test_push_counters_survive_activity_clearing(self) -> None:
        """Count original push payloads even when the visible log is cleared."""
        self.client.next_message.side_effect = [
            Message("updates", b"hello\x1b"),
            Message("updates", b"world"),
            ConnectionError("closed"),
        ]
        await self.workspace._pump()
        self.workspace.activity.clear()
        self.assertEqual(self.workspace.stats.received, 2)
        self.assertFalse(self.workspace.connected)

    async def test_cancellation_releases_queue_without_completing_an_operation(
        self,
    ) -> None:
        """Stopping an in-flight command leaves no unfinished queue bookkeeping."""
        started = asyncio.Event()

        async def set_value(key: str, value: bytes) -> None:
            """Signal execution and remain pending until cancellation."""
            started.set()
            await asyncio.Event().wait()

        self.client.set.side_effect = set_value
        self.workspace.submit(Request("set", "key", b"value"))
        worker = asyncio.create_task(self.workspace._work())
        try:
            async with asyncio.timeout(2):
                await started.wait()
        finally:
            worker.cancel()
            with suppress(asyncio.CancelledError):
                await worker
        async with asyncio.timeout(2):
            await self.workspace._queue.join()
        self.assertEqual(self.workspace.stats.operations, 0)
        self.assertIsNone(self.workspace.stats.average_ms)


class TerminalTests(unittest.TestCase):
    """Check mode changes, editing and inert output without a physical terminal."""

    def setUp(self) -> None:
        """Supply a terminal double and disable optional curses color calls."""
        self.screen = Mock(spec=curses.window)
        self.screen.getmaxyx.return_value = (24, 100)
        self.screen.get_wch.side_effect = curses.error
        self.workspace = Workspace(Mock(spec=Client))
        with patch("velis.tui.view.curses.has_colors", return_value=False):
            self.tui = Tui(self.screen, self.workspace, host="localhost", port=9000)

    def test_stats_shortcut_and_command_work_without_a_connection(self) -> None:
        """Statistics remain local and can be viewed after disconnection."""
        self.workspace.connected = False
        self.tui._handle("t")
        self.assertEqual(self.tui._state.mode, Mode.STATS)
        self.tui._handle("\x1b")
        self.assertEqual(self.tui._state.mode, Mode.BROWSE)
        for key in ":stats\n":
            self.tui._handle(key)
        self.assertEqual(self.tui._state.mode, Mode.STATS)
        self.assertEqual(self.workspace.pending, 0)
        self.assertEqual(self.workspace.stats.operations, 0)

    def test_editing_and_history_restore_the_original_draft(self) -> None:
        """Cursor edits preserve arguments and history preserves an unfinished draft."""
        self.tui._open_line("get key")
        self.tui._edit(curses.KEY_HOME)
        self.tui._edit(curses.KEY_BACKSPACE)
        self.assertEqual(self.tui._state.line, "get key")
        self.tui._edit(curses.KEY_END)
        self.tui._edit("\x17")
        for key in "other\n":
            self.tui._edit(key)
        self.assertEqual(self.workspace._queue.get_nowait(), Request("get", "other"))
        self.tui._open_line("unfinished")
        self.tui._edit(curses.KEY_UP)
        self.assertEqual(self.tui._state.line, "get other")
        self.tui._edit(curses.KEY_DOWN)
        self.assertEqual(self.tui._state.line, "unfinished")

    def test_read_only_rejects_all_write_commands(self) -> None:
        """Every write alias respects read-only mode before submission."""
        self.workspace.read_only = True
        for line in (
            "set key value",
            "setx key ff",
            "delete key",
            "expire key 1s",
            "persist key",
            "publish updates hello",
        ):
            with self.subTest(line=line):
                self.tui._run_line(line)
                self.assertEqual(self.workspace.activity[-1].kind, Kind.ERROR)
        self.assertEqual(self.workspace.pending, 0)

    def test_idle_poll_does_not_schedule_another_frame(self) -> None:
        """An empty input poll must not keep the renderer continuously awake."""
        self.tui._poll()
        self.assertFalse(self.tui._wake.is_set())
        self.screen.get_wch.side_effect = ["t", curses.error()]
        self.tui._poll()
        self.assertTrue(self.tui._wake.is_set())

    def test_all_views_fit_small_terminals_and_escape_prefilled_keys(self) -> None:
        """Keep output inert and within bounds across modes and terminal sizes."""
        key = "wide界\x1b[31m\u202e"
        self.workspace.entries[key] = Entry(key)
        self.workspace.log(Kind.INFO, "notice\x1b")
        self.tui._prefill("set")
        raw_draft = self.tui._state.line
        shown, offset = command_window(raw_draft, len(raw_draft), 99)
        self.assertEqual(shown, clean(raw_draft))
        self.assertEqual(offset, cells(shown))
        for height, width in ((14, 60), (24, 100), (8, 30)):
            for mode in Mode:
                with self.subTest(size=(height, width), mode=mode):
                    self.screen.getmaxyx.return_value = (height, width)
                    self.screen.addstr.reset_mock()
                    self.tui._state.mode = mode
                    with patch("velis.tui.view.curses.ACS_VLINE", 0, create=True):
                        self.tui._renderer.draw()
                    for call in self.screen.addstr.call_args_list:
                        row, col, text, _ = call.args
                        self.assertTrue(text.isprintable())
                        self.assertTrue(0 <= row < height)
                        self.assertTrue(0 <= col < width)
                        self.assertLessEqual(col + cells(text), width)
        self.assertIn("\x1b", raw_draft)
