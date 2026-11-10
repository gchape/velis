"""Parse quoted commands and validate their arguments before submission."""

import re
import shlex
from dataclasses import dataclass

from velis.core.store import MAX_TTL_MS

from .text import label

_UNITS = {"": 1, "ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}
_DURATION = re.compile(r"([0-9]{1,18})(ms|s|m|h|d)?")


def parse_duration(token: str) -> int:
    """Milliseconds from 1500, 1500ms, 30s, 5m, 2h or 1d."""
    match = _DURATION.fullmatch(token.lower())
    if match is None:
        raise ValueError(f"invalid time {token!r}; use 1500, 30s, 5m, 2h or 1d")
    ms = int(match[1]) * _UNITS[match[2] or ""]
    if ms > MAX_TTL_MS:
        raise ValueError("time exceeds the maximum TTL")
    return ms


@dataclass(frozen=True, slots=True)
class Spec:
    """Help text, argument bounds and write permission for a command."""

    name: str
    usage: str
    summary: str
    min_args: int
    max_args: int
    mutates: bool = False


SPECS = {
    spec.name: spec
    for spec in (
        Spec("get", "get KEY", "read a value and its TTL", 1, 1),
        Spec("set", "set KEY VALUE", "store text, clearing any TTL", 2, 2, True),
        Spec("setx", "setx KEY HEX", "store bytes given as hex", 2, 2, True),
        Spec("del", "del KEY", "delete a key", 1, 1, True),
        Spec(
            "expire", "expire KEY TIME", "set a TTL: 1500, 30s, 5m, 2h, 1d", 2, 2, True
        ),
        Spec("ttl", "ttl KEY", "check the remaining TTL", 1, 1),
        Spec("persist", "persist KEY", "remove a TTL", 1, 1, True),
        Spec("sub", "sub CHANNEL", "receive pushes from a channel", 1, 1),
        Spec("unsub", "unsub CHANNEL", "stop receiving a channel", 1, 1),
        Spec("pub", "pub CHANNEL TEXT", "publish text to a channel", 2, 2, True),
        Spec("refresh", "refresh", "re-check every tracked key", 0, 0),
        Spec(
            "forget", "forget [KEY]", "stop tracking a key (default: gone keys)", 0, 1
        ),
        Spec("clear", "clear", "clear the activity log", 0, 0),
        Spec("stats", "stats", "show this session's statistics", 0, 0),
        Spec("help", "help", "show help", 0, 0),
        Spec("quit", "quit", "leave", 0, 0),
    )
}
ALIASES = {
    "delete": "del",
    "rm": "del",
    "subscribe": "sub",
    "unsubscribe": "unsub",
    "publish": "pub",
    "q": "quit",
    "exit": "quit",
    "?": "help",
}
KEY_COMMANDS = frozenset({"get", "set", "setx", "del", "expire", "ttl", "persist"})


class UsageError(ValueError):
    """A command line that cannot be turned into a request."""


@dataclass(frozen=True, slots=True)
class Request:
    """A parsed command: its name and already-converted arguments."""

    name: str
    target: str = ""
    data: bytes = b""
    ms: int = 0


def parse_line(line: str) -> Request:
    """Parse one command line; quote arguments that contain spaces."""
    try:
        words = shlex.split(line)
    except ValueError as exc:
        raise UsageError(f"cannot parse command: {exc}") from exc
    if not words:
        raise UsageError("empty command")
    word = words[0].lower()
    spec = SPECS.get(ALIASES.get(word, word))
    if spec is None:
        raise UsageError(f"unknown command: {label(words[0], 20)} (press ? for help)")
    args = words[1:]
    if not spec.min_args <= len(args) <= spec.max_args:
        raise UsageError(f"usage: {spec.usage}")
    return _parse_args(spec.name, args)


def _parse_args(name: str, args: list[str]) -> Request:
    """Convert validated argument counts into typed request data."""
    target = args[0] if args else ""
    try:
        match name:
            case "set" | "pub":
                return Request(name, target, args[1].encode())
            case "setx":
                return Request(name, target, _parse_hex(args[1]))
            case "expire":
                return Request(name, target, ms=parse_duration(args[1]))
            case _:
                return Request(name, target)
    except UnicodeEncodeError as exc:
        raise UsageError("text is not valid UTF-8") from exc
    except ValueError as exc:
        raise UsageError(str(exc)) from exc


def _parse_hex(token: str) -> bytes:
    """Decode a hexadecimal value with a stable usage error on invalid input."""
    try:
        return bytes.fromhex(token)
    except ValueError as exc:
        raise UsageError("invalid hex") from exc
