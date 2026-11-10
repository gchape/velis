"""Escape untrusted text and format terminal cells, values and durations."""

import codecs
import unicodedata

LOG_CHARS = 200
ELLIPSIS = "…"


def clean(text: str) -> str:
    """Escape every character that is not printable, so output is inert.

    This covers control characters (including ESC), bidi overrides and other
    format characters, and unassigned code points.
    """
    if text.isprintable():
        return text
    return "".join(
        ch if ch.isprintable() else ch.encode("unicode_escape").decode("ascii")
        for ch in text
    )


def cell_width(ch: str) -> int:
    """Terminal cells one printable character occupies."""
    if unicodedata.combining(ch):
        return 0
    return 2 if unicodedata.east_asian_width(ch) in "WF" else 1


def cells(text: str) -> int:
    """Count terminal cells occupied by printable text."""
    return sum(cell_width(ch) for ch in text)


def clip(text: str, width: int) -> str:
    """The longest prefix that fits in width cells; cost is bounded by width."""
    used = 0
    for index, ch in enumerate(text):
        used += cell_width(ch)
        if used > width:
            return text[:index]
    return text


def fit(text: str, width: int) -> str:
    """Fit text into width cells, marking a cut with an ellipsis."""
    if width <= 0:
        return ""
    clipped = clip(text, width)
    if len(clipped) == len(text):
        return text
    return clip(text, width - 1) + ELLIPSIS


def pad(text: str, width: int) -> str:
    """Fit text and pad it to exactly width terminal cells."""
    fitted = fit(text, width)
    return fitted + " " * (width - cells(fitted))


def label(text: str, limit: int = 60) -> str:
    """A short, safe rendering of a key or channel for one-line messages."""
    shown = clean(text[:limit])
    return shown + ELLIPSIS if len(text) > limit else shown


def preview_text(raw: bytes, limit: int = LOG_CHARS) -> str:
    """One safe line of a payload; invalid UTF-8 becomes replacement characters."""
    shown = clean(raw[: limit * 4].decode(errors="replace")[:limit])
    return f"{shown}  [{len(raw)} bytes]" if len(raw) > limit else shown


def command_window(line: str, cursor: int, avail: int) -> tuple[str, int]:
    """The part of a long command line to show and the cursor's cell offset."""
    cursor = len(clean(line[:cursor]))
    line = clean(line)
    start, used = cursor, 0
    while start > 0:
        width = cell_width(line[start - 1])
        if used + width > avail - 1:
            break
        used += width
        start -= 1
    return clip(line[start:], avail), used


def _text_lines(raw: bytes, truncated: bool) -> list[str] | None:
    """Display lines when raw looks like text, else None.

    A preview cut mid-character is still text, so the decoder is incremental.
    """
    try:
        text = codecs.getincrementaldecoder("utf-8")().decode(raw, final=not truncated)
    except UnicodeDecodeError:
        return None
    odd = sum(1 for ch in text if not ch.isprintable() and ch not in "\n\t\r")
    if odd * 10 > len(text):
        return None
    return [
        clean(line.removesuffix("\r").replace("\t", "    "))
        for line in text.split("\n")
    ]


def _hexdump(raw: bytes, per_row: int) -> list[str]:
    """Format bytes as offsets, hexadecimal columns and printable ASCII."""
    rows: list[str] = []
    for offset in range(0, len(raw), per_row):
        chunk = raw[offset : offset + per_row]
        hexes = " ".join(f"{b:02x}" for b in chunk).ljust(per_row * 3 - 1)
        text = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
        rows.append(f"{offset:06x}  {hexes}  {text}")
    return rows


def describe_value(raw: bytes, size: int, width: int) -> list[str]:
    """Safe display lines for a value preview: text if it is text, else hex."""
    lines = _text_lines(raw, size > len(raw))
    if lines is None:
        per_row = max(1, min(16, (width - 10) // 4))
        lines = _hexdump(raw, per_row)
    else:
        lines = [part for line in lines for part in wrap_cells(line, width)]
    if size > len(raw):
        lines.append(f"{ELLIPSIS} {size - len(raw)} more bytes not shown")
    return lines


def format_duration(seconds: float) -> str:
    """Format a duration compactly, from fractions of seconds to days."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, secs = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m{secs:02d}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 48:
        return f"{hours}h{minutes:02d}m"
    return f"{hours // 24}d{hours % 24:02d}h"


def wrap_cells(text: str, width: int) -> list[str]:
    """Wrap printable text by terminal cells without losing any characters."""
    if width <= 0:
        return []
    rows: list[str] = []
    while text:
        part = clip(text, width)
        if len(part) < len(text) and (boundary := part.rfind(" ")) > 0:
            part = part[: boundary + 1]
        if not part:
            part = text[0]
        rows.append(part)
        text = text[len(part) :]
    return rows or [""]
