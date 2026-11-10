"""Verify queue policy and deadlines independently of terminal input."""

import asyncio
import unittest
from contextlib import suppress
from unittest.mock import AsyncMock, Mock, patch

from velis.net.client import Client
from velis.tui.admission import Submission
from velis.tui.commands import Request
from velis.tui.workspace import OP_TIMEOUT_SECONDS, QUEUE_SIZE, Workspace


class AdmissionTests(unittest.IsolatedAsyncioTestCase):
    """Direct callers receive the same protection as keyboard users."""

    def setUp(self) -> None:
        self.now = 100.0
        self.client = Mock(spec=Client)
        self.workspace = Workspace(self.client, monotonic=lambda: self.now)
        self.execute = AsyncMock()
        self.workspace._operations.execute = self.execute

    async def drain(self) -> None:
        """Finish accepted work and always stop the idle worker."""
        worker = asyncio.create_task(self.workspace._work())
        try:
            async with asyncio.timeout(1):
                await self.workspace._queue.join()
        finally:
            worker.cancel()
            with suppress(asyncio.CancelledError):
                await worker

    async def test_direct_read_only_and_invalid_requests_never_enter_queue(
        self,
    ) -> None:
        self.workspace.read_only = True
        for name in ("write", "remove", "lifetime", "keep", "send"):
            self.assertIs(
                self.workspace.submit(Request(name, "name", b"value")),
                Submission.READ_ONLY,
            )
        for name in ("unknown", "help", "status", "clear", "untrack", "quit"):
            self.assertIs(self.workspace.submit(Request(name)), Submission.INVALID)
        self.assertEqual(self.workspace.pending, 0)
        self.execute.assert_not_awaited()
        self.assertFalse(Submission.READ_ONLY)
        self.assertTrue(Submission.ACCEPTED)

    async def test_disconnection_and_capacity_have_distinct_outcomes(self) -> None:
        self.workspace.connected = False
        self.assertIs(
            self.workspace.submit(Request("read", "a")), Submission.DISCONNECTED
        )
        self.workspace.connected = True
        for index in range(QUEUE_SIZE):
            self.assertIs(
                self.workspace.submit(Request("read", str(index))), Submission.ACCEPTED
            )
        self.assertIs(self.workspace.submit(Request("read", "extra")), Submission.FULL)
        await self.drain()
        self.assertEqual(
            [call.args[0].target for call in self.execute.await_args_list],
            [str(index) for index in range(QUEUE_SIZE)],
        )

    async def test_policy_is_rechecked_after_waiting(self) -> None:
        self.workspace.submit(Request("write", "name", b"value"))
        self.workspace.read_only = True
        await self.drain()
        self.execute.assert_not_awaited()
        self.assertIn("read only; not sent", self.workspace.activity[-1].text)
        self.assertEqual(self.workspace.model.stats.operations, 0)

    async def test_stale_write_and_sync_are_discarded_and_release_bookkeeping(
        self,
    ) -> None:
        self.workspace.submit(Request("write", "name", b"value"))
        self.workspace.submit(Request("sync"))
        self.assertIs(self.workspace.submit(Request("sync")), Submission.ACCEPTED)
        self.assertEqual(self.workspace.pending, 2)
        self.now += OP_TIMEOUT_SECONDS
        await self.drain()
        self.execute.assert_not_awaited()
        self.assertTrue(
            all(
                "expired in queue; not sent" in row.text
                for row in self.workspace.activity
            )
        )
        self.assertEqual(self.workspace.model.stats.operations, 0)
        self.assertIs(self.workspace.submit(Request("sync")), Submission.ACCEPTED)
        self.assertEqual(self.workspace.pending, 1)

    async def test_waiting_consumes_execution_budget(self) -> None:
        self.workspace.submit(Request("read", "name"))
        self.now += 4
        queued = self.workspace._queue.get_nowait()
        with patch(
            "velis.tui.workspace.asyncio.timeout", wraps=asyncio.timeout
        ) as timeout:
            await self.workspace._dispatch(queued)
        self.workspace._queue.task_done()
        timeout.assert_called_once_with(OP_TIMEOUT_SECONDS - 4)
        self.execute.assert_awaited_once_with(Request("read", "name"))

    async def test_execution_timeout_reports_uncertain_outcome(self) -> None:
        self.execute.side_effect = TimeoutError
        self.workspace.submit(Request("write", "name", b"value"))
        await self.drain()
        self.assertIn("may still complete", self.workspace.activity[-1].text)
        self.assertEqual(self.workspace.model.stats.errors, 1)
