"""Bounded FIFO reply correlation, independent of transport I/O."""

import asyncio

from velis.core.messages import Reply


class PendingReplies:
    """Reserve ordered reply slots and retain cancelled slots until answered.

    Reservation yields only while acquiring the first slot. The caller must
    write the corresponding frames before its next await, so wire order and
    waiter order cannot diverge. This class owns neither encoding nor sockets.
    """

    def __init__(self, capacity: int, max_batch_bytes: int) -> None:
        self._queue: asyncio.Queue[asyncio.Future[Reply]] = asyncio.Queue(capacity)
        self._max_batch_bytes = max_batch_bytes

    async def reserve(
        self, frames: tuple[bytes, ...], offset: int
    ) -> list[asyncio.Future[Reply]]:
        """Reserve one available window; a single oversized frame is allowed."""
        loop = asyncio.get_running_loop()
        first: asyncio.Future[Reply] = loop.create_future()
        await self._queue.put(first)
        waiters = [first]
        size = len(frames[offset])
        available = self._queue.maxsize - self._queue.qsize()
        stop = min(len(frames), offset + 1 + available)
        for index in range(offset + 1, stop):
            if size + len(frames[index]) > self._max_batch_bytes:
                break
            waiter: asyncio.Future[Reply] = loop.create_future()
            self._queue.put_nowait(waiter)
            waiters.append(waiter)
            size += len(frames[index])
        return waiters

    def resolve(self, reply: Reply) -> bool:
        """Answer the oldest slot, or return False for an unsolicited reply."""
        try:
            waiter = self._queue.get_nowait()
        except asyncio.QueueEmpty:
            return False
        if not waiter.done():
            waiter.set_result(reply)
        return True

    def close(self, reason: str) -> None:
        """Fail pending replies and wake blocked reservations; safe to repeat.

        Retrieve exceptions immediately because callers may have abandoned
        their futures while draining a transport write. Awaiters still receive
        the same exceptions.
        """
        while not self._queue.empty():
            waiter = self._queue.get_nowait()
            if not waiter.done():
                waiter.set_exception(ConnectionError(reason))
                waiter.exception()
        self._queue.shutdown()
