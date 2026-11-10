"""Verify isolated presentation state and command effects across partial failures."""

import unittest
from unittest.mock import Mock

from velis.core.messages import MISSING
from velis.errors import CommandError
from velis.net.client import Client
from velis.tui.commands import Request
from velis.tui.models import Entry, Status
from velis.tui.operations import Operations
from velis.tui.session_model import MAX_TRACKED, SessionModel
from velis.tui.state import ViewState


class SessionModelTests(unittest.TestCase):
    def test_tracking_prefers_discarding_missing_entries_and_stays_bounded(
        self,
    ) -> None:
        model = SessionModel()
        for index in range(MAX_TRACKED):
            model.track(str(index))
        model.entries["3"].set_gone()
        model.track("new")
        self.assertEqual(len(model.entries), MAX_TRACKED)
        self.assertNotIn("3", model.entries)
        self.assertIn("0", model.entries)
        self.assertIs(model.track("new"), model.entries["new"])

    def test_forgetting_missing_entries_preserves_live_previews(self) -> None:
        model = SessionModel()
        model.track("live").set_ttl(None, 0)
        model.track("gone").set_gone()
        self.assertEqual(model.forget(""), 1)
        self.assertEqual(list(model.entries), ["live"])
        self.assertEqual(model.forget("missing"), 0)
        self.assertEqual(model.forget("live"), 1)

    def test_selection_depends_only_on_visible_entries(self) -> None:
        state = ViewState(query="界", selected=100)
        entries = [Entry("hello"), Entry("wide界")]
        self.assertIs(state.selected_entry(entries), entries[1])
        state.query = "missing"
        self.assertIsNone(state.selected_entry(entries))


class OperationTests(unittest.IsolatedAsyncioTestCase):
    async def test_sync_applies_later_successes_after_a_rejected_key(self) -> None:
        client = Mock(spec=Client)
        model = SessionModel()
        for key in ("first", "rejected", "last"):
            model.track(key).set_ttl(None, 0)
        client.remaining_many.return_value = [MISSING, CommandError("denied"), 50]
        log = Mock()
        operations = Operations(client, model, log)
        with self.assertRaisesRegex(CommandError, "1 failed"):
            await operations.execute(Request("sync"))
        self.assertEqual(model.entries["first"].status, Status.GONE)
        self.assertEqual(model.entries["rejected"].status, Status.LIVE)
        self.assertIsNotNone(model.entries["last"].expires_at)
        self.assertTrue(any("denied" in call.args[1] for call in log.call_args_list))
        self.assertEqual(model.stats.operations, 0)
        self.assertEqual(model.stats.hits, 0)
        client.remaining_many.assert_awaited_once_with(["first", "rejected", "last"])
