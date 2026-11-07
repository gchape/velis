class VelisError(Exception):
    """Base class for all velis errors."""


class ProtocolError(VelisError):
    """The byte stream is malformed or desynced; reply ERR, then close."""


class CommandError(VelisError):
    """A well-formed request that can't be honoured; reply ERR, keep going."""
