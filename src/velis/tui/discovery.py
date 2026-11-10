"""Inspect connectivity and stored names without fetching or retaining values."""

import shlex
from collections.abc import Callable

from velis.net.client import Client

from .commands import Request
from .models import Kind, Status
from .session_model import SessionModel
from .text import label


class DiscoveryOperations:
    """Translate lightweight server observations into local tracking and feedback."""

    def __init__(
        self, client: Client, model: SessionModel, log: Callable[[Kind, str], None]
    ) -> None:
        self._client = client
        self._model = model
        self._log = log

    async def ping(self, request: Request) -> None:
        """Confirm a complete round trip; the workspace records its latency."""
        await self._client.ping()
        self._log(Kind.OK, "Pong · server connection is responding")

    async def exists(self, request: Request) -> None:
        """Update existence without claiming to have fetched value or lifetime."""
        found = await self._client.exists(request.target)
        entry = self._model.track(request.target)
        if found:
            entry.status = Status.LIVE
        else:
            entry.set_gone()
        text = "exists" if found else "not found"
        self._log(Kind.OK if found else Kind.INFO, f"{label(request.target)} → {text}")

    async def size(self, request: Request) -> None:
        """Report byte length without marking a value preview as fetched."""
        size = await self._client.size(request.target)
        text = "not found" if size is None else f"{size:,} bytes"
        self._log(Kind.INFO, f"Size {label(request.target)} → {text}")

    async def list_keys(self, request: Request) -> None:
        """Track one name page and make continuation explicit when more remain."""
        page = await self._client.list_keys(request.target, cursor=request.cursor)
        for key in page.keys:
            entry = self._model.track(key)
            if entry.status is Status.GONE:
                entry.status = Status.UNKNOWN
        text = f"Listed {len(page.keys)} key(s)"
        if page.cursor:
            text += f" · next: list {shlex.quote(request.target)} {page.cursor}"
        else:
            text += " · end of listing"
        self._log(Kind.INFO, text)
