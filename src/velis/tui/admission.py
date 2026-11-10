"""Application admission outcomes and deadlines for queued terminal work."""

from dataclasses import dataclass
from enum import Enum, auto

from .commands import SPECS, Request


class Submission(Enum):
    """Why a request was accepted or refused before entering the work queue."""

    ACCEPTED = auto()
    DISCONNECTED = auto()
    FULL = auto()
    READ_ONLY = auto()
    INVALID = auto()

    def __bool__(self) -> bool:
        """Keep boolean admission checks true only for accepted requests."""
        return self is Submission.ACCEPTED


@dataclass(frozen=True, slots=True)
class QueuedRequest:
    """One accepted request and its monotonic end-to-end deadline."""

    request: Request
    deadline: float


def admission(request: Request, *, connected: bool, read_only: bool) -> Submission:
    """Apply command and access policy consistently at submission and execution."""
    spec = SPECS.get(request.name)
    if spec is None or not spec.network:
        return Submission.INVALID
    if read_only and spec.mutates:
        return Submission.READ_ONLY
    if not connected:
        return Submission.DISCONNECTED
    return Submission.ACCEPTED
