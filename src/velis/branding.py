"""Shared static mascot and wordmark for the server and terminal client."""

import os
import sys

_RESET = "\x1b[0m"
_MARGIN = " " * 4


def _fg(r: int, g: int, b: int) -> str:
    return f"\x1b[38;2;{r};{g};{b}m"


HEAD_ROWS = 4

_ROW_COLORS = [
    _fg(183, 157, 252),
    _fg(167, 139, 250),
    _fg(155, 120, 248),
    _fg(139, 100, 240),
    _fg(150, 120, 235),
    _fg(122, 92, 200),
    _fg(91, 63, 158),
]

MASCOT = [
    "    ▄████▄    ",
    " ▄█▀██████▀█▄ ",
    "███▄█▀██▀█▄███",
    "▀▀████▄▄████▀▀",
    "██ ██ ██ ██ ██",
    " ▐█▌▐█▌▐█▌▐█▌ ",
    "   ██    ██   ",
]

WORD = [
    "",
    r" __   __   _ _",
    r" \ \ / /__| (_)___",
    r"  \ V / -_) | (_-<",
    r"   \_/\___|_|_/__/",
    "",
    "",
]


def show_banner() -> None:
    """Print the startup banner if stderr is an interactive terminal.

    A nonempty NO_COLOR environment variable disables the colors.
    """
    if not sys.stderr.isatty():
        return
    color = not os.environ.get("NO_COLOR")
    lines = [
        f"{_MARGIN}{_ROW_COLORS[index]}{art}{_RESET}  {word}"
        if color
        else f"{_MARGIN}{art}  {word}"
        for index, (art, word) in enumerate(zip(MASCOT, WORD, strict=True))
    ]
    print("\n".join(lines), file=sys.stderr)
