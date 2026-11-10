"""Discover server names and metadata without reading values into the workspace."""

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from velis.net.client import Client
from velis.tui.admission import Submission
from velis.tui.commands import Request, UsageError, parse_line
from velis.tui.discovery import DiscoveryOperations
from velis.tui.keyboard import KeyboardController
from velis.tui.models import Status
from velis.tui.operations import Operations
from velis.tui.session_model import SessionModel
from velis.tui.state import Mode, ViewState
from velis.tui.workspace import Workspace


def discovery_client() -> Mock:
    """Provide asynchronous discovery methods while preserving the client contract."""
    client = Mock(spec=Client)
    for name in ("ping", "exists", "size", "list_keys"):
        setattr(client, name, AsyncMock())
    return client


class DiscoveryCommandTests(unittest.TestCase):
    def test_listing_prefix_and_cursor_are_explicit_and_preserve_quoted_names(
        self,
    ) -> None:
        for line, expected in (
            ("ping", Request("ping")),
            ("exists name", Request("exists", "name")),
            ("size name", Request("size", "name")),
            ("list", Request("list")),
            ('list "two words" 42', Request("list", "two words", cursor=42)),
            ('list "" 18446744073709551615', Request("list", cursor=(1 << 64) - 1)),
        ):
            with self.subTest(line=line):
                self.assertEqual(parse_line(line), expected)

    def test_invalid_cursors_are_rejected_before_queueing(self) -> None:
        for token in ("-1", "+1", "1.5", "word", str(1 << 64), "9" * 200):
            with (
                self.subTest(token=token),
                self.assertRaisesRegex(UsageError, "cursor"),
            ):
                parse_line(f'list "" {token}')
        with self.assertRaisesRegex(UsageError, "usage"):
            parse_line("list prefix 0 extra")

    def test_discovery_commands_are_available_in_read_only_mode(self) -> None:
        workspace = Workspace(discovery_client(), read_only=True)
        for line in ("ping", "exists name", "size name", "list"):
            self.assertIs(workspace.submit(parse_line(line)), Submission.ACCEPTED)

    def test_browse_shortcut_opens_listing_without_submitting(self) -> None:
        client = discovery_client()
        workspace = Workspace(client)
        state = ViewState()
        keyboard = KeyboardController(workspace, state, asyncio.Event())
        keyboard.handle("b")
        self.assertEqual(state.mode, Mode.COMMAND)
        self.assertEqual(state.line, "list ")
        self.assertEqual(workspace.pending, 0)
        client.list_keys.assert_not_awaited()


class DiscoveryOperationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.client = discovery_client()
        self.model = SessionModel()
        self.log = Mock()
        self.discovery = DiscoveryOperations(self.client, self.model, self.log)

    async def test_ping_confirms_round_trip_without_changing_key_observations(
        self,
    ) -> None:
        operations = Operations(self.client, self.model, self.log)
        await operations.execute(Request("ping"))
        self.client.ping.assert_awaited_once_with()
        self.assertIn("Pong", self.log.call_args.args[1])
        self.assertFalse(self.model.entries)
        self.assertEqual(self.model.stats.hits, 0)

    async def test_existence_updates_status_without_claiming_value_or_lifetime(
        self,
    ) -> None:
        self.client.exists.side_effect = [True, False]
        await self.discovery.exists(Request("exists", "name"))
        entry = self.model.entries["name"]
        self.assertEqual(entry.status, Status.LIVE)
        self.assertIsNone(entry.size)
        self.assertFalse(entry.ttl_known)
        entry.set_value(b"stale")
        await self.discovery.exists(Request("exists", "name"))
        self.assertEqual(entry.status, Status.GONE)
        self.assertIsNone(entry.size)
        self.assertEqual(entry.preview, b"")

    async def test_zero_size_and_missing_size_do_not_fabricate_value_previews(
        self,
    ) -> None:
        self.client.size.side_effect = [0, None]
        for key in ("empty", "missing"):
            await self.discovery.size(Request("size", key))
        output = [call.args[1] for call in self.log.call_args_list]
        self.assertIn("Size empty → 0 bytes", output)
        self.assertIn("Size missing → not found", output)
        self.assertFalse(self.model.entries)

    async def test_listing_preserves_previews_and_exposes_quoted_continuation(
        self,
    ) -> None:
        known = self.model.track("known")
        known.set_value(b"cached")
        self.model.track("recreated").set_gone()
        self.client.list_keys.return_value = SimpleNamespace(
            cursor=1000, keys=("known", "new", "recreated")
        )
        await self.discovery.list_keys(Request("list", "two words", cursor=7))
        self.client.list_keys.assert_awaited_once_with("two words", cursor=7)
        self.assertEqual(known.preview, b"cached")
        self.assertEqual(self.model.entries["new"].status, Status.UNKNOWN)
        self.assertEqual(self.model.entries["recreated"].status, Status.UNKNOWN)
        self.assertIn("next: list 'two words' 1000", self.log.call_args.args[1])
        self.assertEqual(self.model.stats.hits, 0)

    async def test_empty_page_can_still_offer_continuation(self) -> None:
        self.client.list_keys.return_value = SimpleNamespace(cursor=1000, keys=())
        await self.discovery.list_keys(Request("list"))
        self.assertIn("Listed 0 key(s)", self.log.call_args.args[1])
        self.assertIn("next: list '' 1000", self.log.call_args.args[1])
        self.client.list_keys.return_value = SimpleNamespace(cursor=0, keys=())
        await self.discovery.list_keys(Request("list", cursor=1000))
        self.assertIn("end of listing", self.log.call_args.args[1])
