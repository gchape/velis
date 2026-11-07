import logging
import os
import sys

from velis.config import Config

log = logging.getLogger("velis")

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


def _banner() -> str:
    return "\n".join(f"{m}{_RESET}  {w}" for m, w in zip(_MASCOT, _WORD, strict=True))


def main() -> None:
    config = Config.from_env()
    config.configure_logging()
    if sys.stderr.isatty() and "NO_COLOR" not in os.environ:
        print(_banner(), file=sys.stderr)
    log.info("velis started...")


if __name__ == "__main__":
    main()
