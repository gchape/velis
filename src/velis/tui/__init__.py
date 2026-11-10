"""Terminal client with tracked keys, value previews and session statistics.

Run ``python -m velis.tui`` to connect. The server cannot list keys or expose
store statistics, so the interface reports only this session's observations.
TTL refreshes do not read values or change eviction order; READ requests do.
"""

from .app import main

__all__ = ["main"]
