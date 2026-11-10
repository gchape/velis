"""Scrollable help and statistics documents, independent of input handling."""

import time

from .commands import SPECS
from .models import Status
from .state import Mode, ViewState
from .surface import Surface
from .text import format_duration, wrap_cells
from .workspace import Workspace

_COMMAND_WIDTH = max(len(spec.usage) for spec in SPECS.values())

HELP = (
    "Start here",
    "  :list                discover stored keys (b opens this action)",
    "  :read name           fetch an existing key",
    '  :write name "hello"   create a text value',
    "",
    "Keys",
    "  ↑ ↓  j k    select a key          g G   first / last",
    "  Enter       Read the selected key (a read counts as a use for eviction)",
    "  PgUp PgDn   scroll the value",
    "  w l x p     write / lifetime / remove / keep for the selected key",
    "  r           sync tracked keys    c     clear the activity log",
    "  t           session status",
    "  :           type a command        q     quit",
    "  /           filter local key names (Esc clears)",
    "  b           browse server key names with list",
    "Command line: Tab completes · Enter runs · Esc cancels · ↑ ↓ history",
    "Quote arguments that contain spaces.",
    *(
        f"  {spec.usage:<{_COMMAND_WIDTH}}  {spec.summary}"
        for spec in SPECS.values()
        if spec.name not in {"help", "quit"}
    ),
    "Values are shown escaped: nothing from the server reaches the terminal raw.",
    "The connection is plaintext and unauthenticated; use loopback or a tunnel.",
)


def _milliseconds(value: float | None) -> str:
    """Show an operation latency or a placeholder before the first result."""
    return "—" if value is None else f"{value:.1f} ms"


def statistics(ws: Workspace) -> tuple[str, ...]:
    """Summarize local observations without implying server-wide totals."""
    stats = ws.model.stats
    uptime = format_duration(max(0.0, time.monotonic() - stats.started_at))
    rate = "—" if stats.hit_rate is None else f"{stats.hit_rate:.1f}%"
    live = sum(entry.status is Status.LIVE for entry in ws.model.entries.values())
    gone = sum(entry.status is Status.GONE for entry in ws.model.entries.values())
    unknown = len(ws.model.entries) - live - gone
    return (
        "Session statistics",
        f"Uptime         {uptime}",
        f"Operations     {stats.operations:,}   Errors {stats.errors:,}",
        f"Latency        last {_milliseconds(stats.last_ms)}"
        f"   avg {_milliseconds(stats.average_ms)}",
        f"Reads          hits {stats.hits:,}   misses {stats.misses:,}   hit {rate}",
        f"Messages       sent {stats.published:,}   received {stats.received:,}",
        f"Tracked keys   live {live}   gone {gone}   unknown {unknown}",
        f"Watching       {len(ws.model.channels)}   Queued operations {ws.pending}",
        "Actions include background sync; reads count hits/misses.",
        "Session observations only; server totals are unavailable.",
    )


def draw_overlay(
    surface: Surface, ws: Workspace, state: ViewState, height: int, width: int
) -> None:
    """Wrap and page document content so every help command remains reachable."""
    source = HELP if state.mode is Mode.HELP else statistics(ws)
    lines = [part for line in source for part in wrap_cells(line, width - 4)]
    state.overlay_page = height - 5
    state.overlay_total = len(lines)
    state.overlay_scroll = min(
        state.overlay_scroll, max(0, len(lines) - state.overlay_page)
    )
    title = " Keyboard & commands" if state.mode is Mode.HELP else " Session statistics"
    surface.heading(2, 0, title, width)
    start = state.overlay_scroll
    for row, line in enumerate(lines[start : start + state.overlay_page]):
        surface.put(3 + row, 2, line, width - 4)
    end = min(len(lines), start + state.overlay_page)
    hint = f" ↑↓ scroll · PgUp/PgDn page · Esc back   {start + 1}-{end}/{len(lines)}"
    surface.put(height - 1, 0, hint, width, surface.theme.dim)
