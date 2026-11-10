"""Interpret client replies consistently for individual and batched commands.

Wire decoding establishes message structure; these helpers check the reply's
meaning for a command. Server rejections raise CommandError, while unexpected
reply types raise ProtocolError. Raw batch results remain available to callers.
"""

from velis.core.messages import (
    MISSING,
    NO_EXPIRY,
    Err,
    Nil,
    RemainingValue,
    Reply,
    Unlimited,
    Value,
)
from velis.errors import CommandError, ProtocolError


def unexpected_reply(command: str, reply: Reply) -> ProtocolError:
    """Describe a reply that does not satisfy the named command's contract."""
    return ProtocolError(f"unexpected reply to {command}: {reply!r}")


def expect_reply[R: Reply](command: str, reply: Reply, *accepted: type[R]) -> R:
    """Raise server errors and require one of the accepted reply types."""
    if isinstance(reply, Err):
        raise CommandError(reply.message)
    if not isinstance(reply, accepted):
        raise unexpected_reply(command, reply)
    return reply


def value_of(reply: Reply) -> bytes | None:
    """Interpret a READ reply, preserving empty bytes and mapping Nil to None."""
    result = expect_reply("READ", reply, Value, Nil)
    return result.value if isinstance(result, Value) else None


def remaining_of(reply: Reply) -> int:
    """Interpret TTL as milliseconds, MISSING (-2), or NO_EXPIRY (-1)."""
    match expect_reply("REMAINING", reply, RemainingValue, Unlimited, Nil):
        case RemainingValue(ms):
            return ms
        case Unlimited():
            return NO_EXPIRY
        case _:
            return MISSING
