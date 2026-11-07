import os
import sys

_HEAD = "\x1b[38;2;167;139;250m"
_BODY = "\x1b[38;2;91;63;158m"
_RESET = "\x1b[0m"

_MASCOT = [
    _HEAD + " ▄████▄ ",
    _HEAD + "████████",
    _HEAD + "██ ██ ██",
    _HEAD + "██▀▄▄▀██",
    _BODY + "█▀█▀█▀█▀",
    _BODY + " ▀ ▀ ▀ ▀",
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
    """Print the startup banner if stderr is an interactive terminal."""
    if not sys.stderr.isatty() or "NO_COLOR" in os.environ:
        return
    print(
        "\n".join((f"{m}{_RESET}  {w}" for m, w in zip(_MASCOT, _WORD, strict=True))),
        file=sys.stderr,
    )
