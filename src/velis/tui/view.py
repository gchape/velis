"""Render tracked keys, value previews, activity, help and session statistics."""

import curses
import os
import textwrap
import time
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum, auto

from velis.branding import HEAD_ROWS, MASCOT, WORD

from .commands import SPECS
from .models import MARKS, Entry, Kind, Status, ttl_text
from .text import (
    clean,
    command_window,
    describe_value,
    fit,
    format_duration,
    label,
    pad,
)
from .workspace import Workspace

NOTICE_SECONDS = 6.0
MIN_ROWS, MIN_COLS = 14, 60


class Mode(Enum):
    """Active browse, command or informational screen."""

    BROWSE = auto()
    COMMAND = auto()
    HELP = auto()
    STATS = auto()


@dataclass(slots=True)
class ViewState:
    """Selection, scrolling and command draft shared by input and rendering."""

    mode: Mode = Mode.BROWSE
    line: str = ""
    cursor: int = 0
    selected: int = 0
    top: int = 0
    scroll: int = 0
    page: int = 10
    shown: str | None = None
    follow: str | None = None

    def selected_entry(self, workspace: Workspace) -> Entry | None:
        """Return the selected key, clamping after keys have been forgotten."""
        entries = list(workspace.entries.values())
        if not entries:
            return None
        return entries[max(0, min(self.selected, len(entries) - 1))]

    def keep_visible(self, entries: list[Entry], visible: int) -> None:
        """Follow a submitted key and keep the selection inside the key pane."""
        if self.follow is not None:
            for index, entry in enumerate(entries):
                if entry.key == self.follow:
                    self.selected = index
                    break
            self.follow = None
        self.selected = max(0, min(self.selected, len(entries) - 1))
        self.top = max(self.selected - visible + 1, min(self.top, self.selected))
        self.top = max(0, min(self.top, max(0, len(entries) - visible)))


INTRO = (
    "Press : and type set name value or get name to start. "
    "Only keys touched in this session are listed."
)

HELP = (
    "Keys",
    "  ↑ ↓  j k    select a key          g G   first / last",
    "  Enter       GET the selected key (a read counts as a use for eviction)",
    "  PgUp PgDn   scroll the value",
    "  s e d p     start a set / expire / del / persist command for the key",
    "  r           re-check every key    c     clear the activity log",
    "  t           session statistics",
    "  :           type a command        q     quit",
    "Command line: Enter runs · Esc cancels · ↑ ↓ history · quote args with spaces",
    *(
        f"  {spec.usage:<20}{spec.summary}"
        for spec in SPECS.values()
        if spec.name not in {"help", "quit"}
    ),
    "Values are shown escaped: nothing from the server reaches the terminal raw.",
    "The connection is plaintext and unauthenticated; use loopback or a tunnel.",
)


def _value_lines(entry: Entry, width: int) -> list[str]:
    """Describe a cached value without fetching it or changing eviction state."""
    if entry.status is Status.GONE:
        return ["(no value)"]
    if entry.size is None:
        return [
            "Press Enter to fetch the value.",
            "A read counts as a use for eviction.",
        ]
    if entry.size == 0:
        return ["(empty value)"]
    return describe_value(entry.preview, entry.size, width)


def _milliseconds(value: float | None) -> str:
    """Show an operation latency or a placeholder before the first result."""
    return "—" if value is None else f"{value:.1f} ms"


class Renderer:
    """Render shared interface state without handling input or sending requests."""

    def __init__(
        self,
        scr: curses.window,
        workspace: Workspace,
        state: ViewState,
        *,
        host: str,
        port: int,
    ) -> None:
        """Bind the terminal, workspace and shared display state."""
        self._scr = scr
        self._ws = workspace
        self._state = state
        self._where = f"{host}:{port}"
        self._cursor_visible: bool | None = None
        self._init_styles()

    def _init_styles(self) -> None:
        """Choose readable attributes with optional terminal color support."""
        bold, reverse = curses.A_BOLD, curses.A_REVERSE
        self._title = reverse | bold
        self._pane = bold | curses.A_UNDERLINE
        self._dim = curses.A_DIM
        self._brand_head, self._brand_body = bold, self._dim
        self._styles = dict.fromkeys(Kind, 0)
        self._styles[Kind.ERROR] = bold
        if not curses.has_colors():
            return
        try:
            curses.use_default_colors()
            background = -1
        except curses.error:
            background = curses.COLOR_BLACK
        for number, (kind, color) in enumerate(
            (
                (Kind.OK, curses.COLOR_GREEN),
                (Kind.WARN, curses.COLOR_YELLOW),
                (Kind.ERROR, curses.COLOR_RED),
                (Kind.PUSH, curses.COLOR_CYAN),
            ),
            start=1,
        ):
            curses.init_pair(number, color, background)
            self._styles[kind] = curses.color_pair(number) | (
                bold if kind is Kind.ERROR else 0
            )
        self._init_brand(background)

    def _init_brand(self, background: int) -> None:
        """Use two purple colors for the shared static mascot."""
        if os.environ.get("NO_COLOR"):
            return
        head, body = (
            (141, 61)
            if curses.COLORS >= 256
            else (curses.COLOR_MAGENTA, curses.COLOR_MAGENTA)
        )
        curses.init_pair(5, head, background)
        curses.init_pair(6, body, background)
        self._brand_head = curses.color_pair(5)
        self._brand_body = curses.color_pair(6)

    def _put(self, y: int, x: int, text: str, width: int, attr: int = 0) -> None:
        """Escape and fit every terminal write, tolerating resize races."""
        if width > 0:
            with suppress(curses.error):
                self._scr.addstr(y, x, fit(clean(text), width), attr)

    def draw(self) -> None:
        """Draw one frame for the current mode and terminal size."""
        scr = self._scr
        scr.erase()
        height, width = scr.getmaxyx()
        if height < MIN_ROWS or width < MIN_COLS:
            self._put(0, 0, f"terminal too small (need {MIN_COLS}x{MIN_ROWS})", width)
            scr.refresh()
            return
        self._draw_header(width)
        if self._state.mode is Mode.HELP:
            self._draw_help(height, width)
        elif self._state.mode is Mode.STATS:
            self._draw_stats(height, width)
        else:
            self._draw_summary(width)
            self._draw_workspace(height, width)
            self._draw_footer(height, width)
        self._place_cursor(height, width)
        scr.refresh()

    def _draw_workspace(self, height: int, width: int) -> None:
        """Lay out key, value and activity panes beneath the session summary."""
        body_top, body_rows = 2, height - 4
        keys_w = min(max(width // 3, 26), 44)
        right_x = keys_w + 1
        detail_rows = max(7, body_rows * 6 // 10)
        self._draw_keys(body_top, body_rows, keys_w)
        with suppress(curses.error):
            self._scr.vline(body_top, keys_w, curses.ACS_VLINE, body_rows)
        self._draw_detail(body_top, right_x, detail_rows, width - right_x)
        self._draw_activity(
            body_top + detail_rows, right_x, body_rows - detail_rows, width - right_x
        )

    def _draw_header(self, width: int) -> None:
        """Show connection status, last latency and session guard settings."""
        ws = self._ws
        parts = [
            f" velis {self._where}",
            "connected" if ws.connected else "DISCONNECTED",
        ]
        if ws.stats.last_ms is not None:
            parts.append(f"last op {ws.stats.last_ms:.1f} ms")
        if ws.channels:
            parts.append(f"subscribed {len(ws.channels)}")
        if ws.pending:
            parts.append(f"pending {ws.pending}")
        if ws.read_only:
            parts.append("READ-ONLY")
        style = self._title if ws.connected else self._title | self._styles[Kind.ERROR]
        self._put(0, 0, pad("  ·  ".join(parts), width), width, style)

    def _draw_summary(self, width: int) -> None:
        """Keep the most useful session counters visible while browsing."""
        stats = self._ws.stats
        text = (
            f" Session · ops {stats.operations} · errors {stats.errors}"
            f" · hits {stats.hits} · misses {stats.misses} · pushes {stats.received}"
        )
        self._put(1, 0, text, width, self._dim)

    def _draw_stats(self, height: int, width: int) -> None:
        """Show local observations without implying server-wide statistics."""
        ws, stats = self._ws, self._ws.stats
        uptime = format_duration(max(0.0, time.monotonic() - stats.started_at))
        rate = "—" if stats.hit_rate is None else f"{stats.hit_rate:.1f}%"
        live = sum(entry.status is Status.LIVE for entry in ws.entries.values())
        gone = sum(entry.status is Status.GONE for entry in ws.entries.values())
        unknown = len(ws.entries) - live - gone
        lines = (
            "Session statistics",
            f"Uptime         {uptime}",
            f"Operations     {stats.operations:,}   Errors {stats.errors:,}",
            f"Latency        last {_milliseconds(stats.last_ms)}"
            f"   avg {_milliseconds(stats.average_ms)}",
            f"GET            hits {stats.hits:,}   misses {stats.misses:,}"
            f"   hit {rate}",
            f"Pub/sub        published {stats.published:,}"
            f"   received {stats.received:,}",
            f"Tracked keys   live {live}   gone {gone}   unknown {unknown}",
            f"Subscriptions  {len(ws.channels)}   Queued operations {ws.pending}",
            "Operations include TTL refreshes; GET counts hits/misses.",
            "Session observations only; server totals are unavailable.",
        )
        for row, line in enumerate(lines, start=2):
            self._put(row, 1, line, width - 2, self._pane if row == 2 else 0)
        self._put(height - 1, 0, " Any key returns to browsing", width, self._dim)

    def _draw_keys(self, top: int, rows: int, width: int) -> None:
        """Draw tracked keys with selection, status markers and TTL countdowns."""
        entries = list(self._ws.entries.values())
        self._put(top, 0, pad(f" Keys ({len(entries)})", width), width, self._pane)
        visible = rows - 1
        self._state.keep_visible(entries, visible)
        now = time.monotonic()
        ttl_w = 8
        key_w = width - 4 - ttl_w
        for row, entry in enumerate(
            entries[self._state.top : self._state.top + visible]
        ):
            index = self._state.top + row
            marker = {Status.LIVE: "●", Status.GONE: "○", Status.UNKNOWN: "·"}[
                entry.status
            ]
            text = (
                f" {marker} {pad(clean(entry.key), key_w)} "
                f"{ttl_text(entry, now).rjust(ttl_w)}"
            )
            if index == self._state.selected:
                attr = curses.A_REVERSE
            else:
                attr = self._dim if entry.status is not Status.LIVE else 0
            self._put(top + 1 + row, 0, pad(text, width), width, attr)

    def _draw_detail(self, top: int, x: int, rows: int, width: int) -> None:
        """Show selected key metadata and the currently cached value preview."""
        self._put(top, x, pad(" Value", width), width, self._pane)
        entry = self._state.selected_entry(self._ws)
        if entry is None:
            self._draw_empty(top, x, rows, width)
            return
        key = entry.key
        if key != self._state.shown:
            self._state.shown, self._state.scroll = key, 0
        now = time.monotonic()
        state = {
            Status.LIVE: "live",
            Status.GONE: "gone (expired, evicted or deleted)",
            Status.UNKNOWN: "unknown",
        }[entry.status]
        size = "not fetched" if entry.size is None else f"{entry.size:,} bytes"
        header = (
            f"Key    {clean(key)}",
            f"State  {state}",
            f"TTL    {ttl_text(entry, now)}",
            f"Size   {size}",
        )
        for row, line in enumerate(header):
            self._put(top + 1 + row, x + 1, line, width - 1)
        self._draw_value_body(entry, top, x, rows, width)

    def _draw_empty(self, top: int, x: int, rows: int, width: int) -> None:
        """Show shared branding and getting-started text when the pane has room."""
        lines = textwrap.wrap(INTRO, max(10, width - 2))
        mascot_width = max(map(len, MASCOT))
        brand_width = mascot_width + 2 + max(map(len, WORD))
        if rows - 1 >= len(MASCOT) + 1 + len(lines) and width - 2 >= brand_width:
            word_x = x + mascot_width + 3
            for row, (art, word) in enumerate(zip(MASCOT, WORD, strict=True)):
                attr = self._brand_head if row < HEAD_ROWS else self._brand_body
                self._put(top + 1 + row, x + 1, art, width - 2, attr)
                self._put(top + 1 + row, word_x, word, x + width - word_x)
            top += len(MASCOT) + 1
            rows -= len(MASCOT) + 1
        for row, line in enumerate(lines[: rows - 1]):
            self._put(top + 1 + row, x + 1, line, width - 1, self._dim)

    def _draw_value_body(
        self, entry: Entry, top: int, x: int, rows: int, width: int
    ) -> None:
        """Scroll a bounded preview inside the space below the metadata rows."""
        body = _value_lines(entry, width - 2)
        body_top = top + 6
        body_rows = max(0, top + rows - body_top)
        self._state.page = max(1, body_rows - 1)
        self._state.scroll = max(0, min(self._state.scroll, len(body) - body_rows))
        for row, line in enumerate(
            body[self._state.scroll : self._state.scroll + body_rows]
        ):
            self._put(body_top + row, x + 1, line, width - 1)
        if len(body) > body_rows:
            end = min(len(body), self._state.scroll + body_rows)
            more = f" {self._state.scroll + 1}-{end} of {len(body)}"
            self._put(top, x + width - len(more) - 1, more, len(more) + 1, self._pane)

    def _draw_activity(self, top: int, x: int, rows: int, width: int) -> None:
        """Show the latest activity and subscribed channels."""
        subs = ", ".join(sorted(label(c, 30) for c in self._ws.channels))
        title = " Activity" + (f"   subscribed: {subs}" if subs else "")
        self._put(top, x, pad(title, width), width, self._pane)
        visible = rows - 1
        if visible <= 0:
            return
        for row, item in enumerate(list(self._ws.activity)[-visible:]):
            text = f"{item.when} {MARKS[item.kind]} {item.text}"
            self._put(top + 1 + row, x + 1, text, width - 1, self._styles[item.kind])

    def _draw_footer(self, height: int, width: int) -> None:
        """Show the latest notice and either a command draft or browse hints."""
        for item in reversed(self._ws.activity):
            if item.kind is not Kind.PUSH:
                if time.monotonic() - item.at < NOTICE_SECONDS:
                    text = f" {MARKS[item.kind]} {item.text}"
                    self._put(height - 2, 0, text, width, self._styles[item.kind])
                break
        if self._state.mode is Mode.COMMAND:
            shown, _ = command_window(self._state.line, self._state.cursor, width - 1)
            self._put(height - 1, 0, ":" + shown, width)
        else:
            hint = " ↑↓ select  Enter get  t stats  : command  ? help  q quit"
            self._put(height - 1, 0, hint, width, self._dim)

    def _draw_help(self, height: int, width: int) -> None:
        """Draw a help overlay sized to the available terminal."""
        inner = min(width - 4, 80)
        box_h = min(len(HELP) + 2, height - 2)
        left, top = (width - inner - 2) // 2, (height - box_h) // 2
        self._put(top, left, "┌" + "─" * inner + "┐", inner + 2)
        for row in range(box_h - 2):
            self._put(
                top + 1 + row, left, "│" + pad(" " + HELP[row], inner) + "│", inner + 2
            )
        closing = "─ any key closes "
        self._put(
            top + box_h - 1, left, "└" + closing.ljust(inner, "─") + "┘", inner + 2
        )

    def _place_cursor(self, height: int, width: int) -> None:
        """Position the command cursor and hide it outside command mode."""
        editing = self._state.mode is Mode.COMMAND
        if editing:
            _, used = command_window(self._state.line, self._state.cursor, width - 1)
            with suppress(curses.error):
                self._scr.move(height - 1, min(width - 1, 1 + used))
        if self._cursor_visible != editing:
            with suppress(curses.error):
                curses.curs_set(1 if editing else 0)
            self._cursor_visible = editing
