class VelisError(Exception):
    """Base class for all velis errors."""


class ProtocolError(VelisError):
    """A value or byte stream violates the configured wire representation.

    Invalid outgoing values fail before writing and leave the stream usable.
    Invalid incoming data requires closing the stream; servers first try ERR.
    """


class CommandError(VelisError):
    """A well-formed request that can't be honoured; reply ERR, keep going."""


class CapacityError(CommandError):
    """A write or subscription would exceed a configured resource limit."""
