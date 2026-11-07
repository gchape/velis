import os
import sys

_HEAD = "\x1b[38;2;167;139;250m"
_BODY = "\x1b[38;2;91;63;158m"
_RESET = "\x1b[0m"

_MASCOT = [
    (_HEAD, " ▄████▄ "),
    (_HEAD, "████████"),
    (_HEAD, "██ ██ ██"),
    (_HEAD, "██▀▄▄▀██"),
    (_BODY, "█▀█▀█▀█▀"),
    (_BODY, " ▀ ▀ ▀ ▀"),
]

_WORD = [
    "",
    r" __   __   _ _",
    r" \ \ / /__| (_)___",
    r"  \ V / -_) | (_-<",
    r"   \_/\___|_|_/__/",
    "",
]


def show() -> None:
    """Print the startup banner if stderr is an interactive terminal.

    NO_COLOR (https://no-color.org) strips the colors, not the banner.
    """
    if not sys.stderr.isatty():
        return
    color = not os.environ.get("NO_COLOR")
    lines = [
        f"{tint}{art}{_RESET}  {word}" if color else f"{art}  {word}"
        for (tint, art), word in zip(_MASCOT, _WORD, strict=True)
    ]
    print("\n".join(lines), file=sys.stderr)
