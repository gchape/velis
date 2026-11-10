"""Connection status, command guidance and keyboard hints."""

import time

from .commands import command_names, command_spec
from .models import MARKS, Kind
from .state import Mode, ViewState
from .surface import Surface
from .text import command_window, pad
from .workspace import Workspace

NOTICE_SECONDS = 6.0


def draw_header(surface: Surface, ws: Workspace, where: str, width: int) -> None:
    """Prioritize connection and permission state over secondary counters."""
    status = "connected" if ws.connected else "DISCONNECTED"
    access = " · READ-ONLY" if ws.read_only else ""
    title = f" velis / workspace · {status}{access} · {where}"
    surface.put(0, 0, pad(title, width), width, surface.theme.title)
    stats = ws.model.stats
    summary = f" Session  {stats.operations} actions · {stats.errors} errors"
    if stats.last_ms is not None:
        summary += f" · {stats.last_ms:.1f} ms"
    if ws.pending:
        summary += f" · {ws.pending} queued"
    if ws.model.channels:
        summary += f" · {len(ws.model.channels)} subscriptions"
    surface.put(1, 0, summary, width, surface.theme.dim)


def command_hint(line: str, read_only: bool) -> str:
    """Offer the current command's usage without rejecting unfinished input."""
    words = line.split(maxsplit=1)
    if not words:
        return " Type an action · Tab complete · Enter run · Esc cancel"
    name = words[0].lower()
    spec = command_spec(name)
    if spec is None:
        return " ? help lists commands · Enter run · Esc cancel"
    if read_only and spec.mutates:
        return f" Read-only: {spec.name} is disabled · Esc cancel"
    return f" {spec.usage} · {spec.summary}"


def draw_footer(
    surface: Surface, ws: Workspace, state: ViewState, height: int, width: int
) -> None:
    """Keep the next action visible and show command-specific input guidance."""
    if state.mode is Mode.FILTER:
        _filter_footer(surface, state, height, width)
        return
    if state.mode is Mode.COMMAND:
        surface.put(
            height - 2,
            0,
            command_hint(state.line, ws.read_only),
            width,
            surface.theme.dim,
        )
        shown, used = command_window(state.line, state.cursor, width - 2)
        surface.put(height - 1, 0, ": " + shown, width)
        surface.cursor((height - 1, min(width - 1, 2 + used)))
        return
    _notice(surface, ws, height - 2, width)
    hint = " ↑↓ select · Enter read · / filter · : actions · ? help"
    if width >= 90:
        hint = (
            " ↑↓ select · Enter read · / filter · PgUp/PgDn value"
            " · : actions · t status · ? help · q quit"
        )
    surface.put(height - 1, 0, hint, width, surface.theme.dim)
    surface.cursor(None)


def _notice(surface: Surface, ws: Workspace, row: int, width: int) -> None:
    """Show recent command feedback, then fall back to useful browse actions."""
    for item in reversed(ws.activity):
        if item.kind is not Kind.PUSH:
            if time.monotonic() - item.at < NOTICE_SECONDS:
                surface.put(
                    row,
                    0,
                    f" {MARKS[item.kind]} {item.text}",
                    width,
                    surface.theme.activity[item.kind],
                )
                return
            break
    hint = (
        " r sync · t status · Esc clear filter"
        if ws.read_only
        else " w write · l lifetime · x remove · p keep · r sync"
    )
    surface.put(row, 0, hint, width, surface.theme.dim)


def draw_context(surface: Surface, ws: Workspace, state: ViewState, width: int) -> None:
    """Use the context strip for action discovery or active filtering."""
    if state.mode is Mode.COMMAND:
        prefix = state.line.split(maxsplit=1)[0] if state.line else ""
        names = command_names(prefix, read_only=ws.read_only)
        text = (
            " Actions  " + " · ".join(names) if names else " Enter runs · Esc cancels"
        )
    elif state.query:
        count = len(state.visible_entries(list(ws.model.entries.values())))
        text = f" Filter: {state.query} · {count} matches · Esc clears"
    else:
        text = " b browse server keys · / filter names · : actions"
    surface.put(2, 0, text, width, surface.theme.dim)


def _filter_footer(surface: Surface, state: ViewState, height: int, width: int) -> None:
    """Show the search prompt while previewing matches in place."""
    surface.put(
        height - 2,
        0,
        " Filter names · Enter keep · Esc cancel · Ctrl+U clear",
        width,
        surface.theme.dim,
    )
    shown, used = command_window(state.query, len(state.query), width - 3)
    surface.put(height - 1, 0, "/ " + shown, width)
    surface.cursor((height - 1, min(width - 1, 2 + used)))
