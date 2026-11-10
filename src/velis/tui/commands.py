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
    network: bool = True


SPECS = {
    spec.name: spec
    for spec in (
        Spec("ping", "ping", "check the server connection", 0, 0),
        Spec("list", "list [PREFIX] [CURSOR]", "browse stored key names", 0, 2),
        Spec("read", "read KEY", "read a value and its remaining lifetime", 1, 1),
        Spec("exists", "exists KEY", "check whether a key exists", 1, 1),
        Spec("size", "size KEY", "show a value's byte length", 1, 1),
        Spec(
            "write", "write KEY TEXT", "store text and clear its lifetime", 2, 2, True
        ),
        Spec("remove", "remove KEY", "remove a stored key", 1, 1, True),
        Spec(
            "lifetime",
            "lifetime KEY TIME",
            "expire after 30s, 5m, 2h or 1d",
            2,
            2,
            True,
        ),
        Spec("remaining", "remaining KEY", "check a key's remaining lifetime", 1, 1),
        Spec("keep", "keep KEY", "remove the lifetime limit", 1, 1, True),
        Spec("watch", "watch CHANNEL", "listen for messages on a channel", 1, 1),
        Spec("unwatch", "unwatch CHANNEL", "stop listening to a channel", 1, 1),
        Spec(
            "send",
            "send CHANNEL TEXT",
            "send a message to channel listeners",
            2,
            2,
            True,
        ),
        Spec("sync", "sync", "update the status of tracked keys", 0, 0),
        Spec(
            "untrack",
            "untrack [KEY]",
            "hide a key, or all missing keys",
            0,
            1,
            network=False,
        ),
        Spec(
            "clear",
            "clear",
            "clear activity without resetting statistics",
            0,
            0,
            network=False,
        ),
        Spec("status", "status", "show this session's statistics", 0, 0, network=False),
        Spec("help", "help", "show commands and keyboard help", 0, 0, network=False),
        Spec("quit", "quit", "leave the workspace", 0, 0, network=False),
    )
}
ALIASES = {"q": "quit", "exit": "quit", "?": "help"}
KEY_COMMANDS = frozenset(
    {"read", "write", "remove", "lifetime", "remaining", "keep", "exists", "size"}
)


def command_spec(token: str) -> Spec | None:
    """Resolve a command word or navigation alias without parsing arguments."""
    token = token.lower()
    return SPECS.get(ALIASES.get(token, token))


def command_names(prefix: str, *, read_only: bool = False) -> tuple[str, ...]:
    """List discoverable commands matching a prefix and the current access mode."""
    return tuple(
        sorted(
            spec.name
            for spec in SPECS.values()
            if spec.name.startswith(prefix.lower()) and not (read_only and spec.mutates)
        )
    )


class UsageError(ValueError):
    """A command line that cannot be turned into a request."""


@dataclass(frozen=True, slots=True)
class Request:
    """A parsed command: its name and already-converted arguments."""

    name: str
    target: str = ""
    data: bytes = b""
    ms: int = 0
    cursor: int = 0


def parse_line(line: str) -> Request:
    """Parse one command line; quote arguments that contain spaces."""
    try:
        words = shlex.split(line)
    except ValueError as exc:
        raise UsageError(f"cannot parse command: {exc}") from exc
    if not words:
        raise UsageError("empty command")
    spec = command_spec(words[0])
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
            case "write" | "send":
                return Request(name, target, args[1].encode())
            case "lifetime":
                return Request(name, target, ms=parse_duration(args[1]))
            case "list":
                cursor = _parse_cursor(args[1]) if len(args) == 2 else 0
                return Request(name, target, cursor=cursor)
            case _:
                return Request(name, target)
    except UnicodeEncodeError as exc:
        raise UsageError("text is not valid UTF-8") from exc
    except ValueError as exc:
        raise UsageError(str(exc)) from exc


def _parse_cursor(token: str) -> int:
    """Accept an unsigned decimal wire cursor with a concise usage error."""
    if re.fullmatch(r"[0-9]{1,20}", token) is None or int(token) >= 1 << 64:
        raise ValueError("cursor must be an integer between 0 and 18446744073709551615")
    return int(token)
