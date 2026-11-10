"""Bounded command dispatch, independent of socket ownership and scheduling.

The caller owns the input buffer and commits the consumed prefix after dispatch.
Execution never yields; reply order therefore matches command order. Transport
closure is observed through a callback because publishing may close a connection
while a command is running.
"""

from collections.abc import Callable
from dataclasses import dataclass

from velis.core.broker import Subscriber
from velis.core.messages import Command, Err, Reply
from velis.errors import CommandError, ProtocolError
from velis.net import protocol

COMMANDS_PER_TURN = 64
REPLY_BYTES_PER_TURN = 1 << 18


@dataclass(slots=True)
class CommandBatch:
    """Completed replies and consumed input, plus any subsequent protocol error.

    ``limited`` means a per-turn limit ended the batch, so more complete
    commands may be buffered even though none was left unhandled by an error.
    """

    replies: list[bytes]
    consumed: int = 0
    limited: bool = False
    error: ProtocolError | None = None


def execute_batch(
    buffer: bytearray,
    execute: Callable[[Command, Subscriber], Reply],
    subscriber: Subscriber,
    stopped: Callable[[], bool],
) -> CommandBatch:
    """Execute a bounded complete prefix and retain replies before a wire error.

    Command errors become ordinary replies and allow subsequent commands.
    Malformed frames end the batch; unexpected execution errors propagate so
    the owning connection can abort. Partial trailing frames remain untouched.
    """
    decode = protocol.COMMANDS.decode
    encode = protocol.encode_message
    replies: list[bytes] = []
    consumed = queued = 0
    limited = False
    error: ProtocolError | None = None
    try:
        for start, end in protocol.iter_frame_spans(buffer):
            command = decode(buffer, start, end)
            try:
                reply = execute(command, subscriber)
            except CommandError as exc:
                reply = Err(str(exc))
            data = encode(reply)
            replies.append(data)
            consumed = end
            queued += len(data)
            if len(replies) == COMMANDS_PER_TURN or queued >= REPLY_BYTES_PER_TURN:
                limited = True
                break
            if stopped():
                break
    except ProtocolError as exc:
        error = exc
    return CommandBatch(replies, consumed, limited, error)
