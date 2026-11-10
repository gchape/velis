"""Connect a curses terminal client to a Velis server."""

import argparse
import asyncio
import curses
import ipaddress
import locale
import os
import sys
from collections.abc import Generator
from contextlib import contextmanager, suppress

from velis.net.client import Client
from velis.net.defaults import DEFAULT_HOST, DEFAULT_PORT

from .models import Kind
from .ui import Tui
from .workspace import Workspace


def is_loopback(host: str) -> bool:
    """Recognize localhost and loopback IP literals without resolving DNS."""
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


@contextmanager
def _terminal() -> Generator[curses.window]:
    """Enter curses mode and always restore the terminal."""
    scr = curses.initscr()
    try:
        curses.noecho()
        curses.cbreak()
        with suppress(curses.error):
            curses.set_escdelay(25)
        scr.keypad(True)
        scr.nodelay(True)
        if curses.has_colors():
            curses.start_color()
        yield scr
    finally:
        scr.keypad(False)
        curses.nocbreak()
        curses.echo()
        with suppress(curses.error):
            curses.curs_set(1)
        curses.endwin()


def _port(text: str) -> int:
    """Parse a TCP port with an argparse-friendly validation error."""
    try:
        port = int(text)
    except ValueError:
        port = 0
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError(f"invalid port: {text!r}")
    return port


async def _run(host: str, port: int, read_only: bool) -> None:
    """Own the network client and terminal for the lifetime of the interface."""
    try:
        client = await Client.connect(host, port)
    except OSError as exc:
        raise SystemExit(f"cannot connect to {host}:{port}: {exc}") from exc
    async with client:
        workspace = Workspace(client, read_only=read_only)
        workspace.log(Kind.INFO, f"connected to {host}:{port}; press ? for help")
        if not is_loopback(host):
            workspace.log(
                Kind.WARN,
                "plaintext, unauthenticated link: anyone on the path can read it",
            )
        with _terminal() as scr:
            await Tui(scr, workspace, host=host, port=port).run()


def main() -> None:
    """Parse options (defaults honour VELIS_HOST and VELIS_PORT) and run."""
    parser = argparse.ArgumentParser(prog="velis-tui", description=__doc__)
    parser.add_argument("--host", default=os.environ.get("VELIS_HOST", DEFAULT_HOST))
    parser.add_argument(
        "--port", type=_port, default=os.environ.get("VELIS_PORT", str(DEFAULT_PORT))
    )
    parser.add_argument(
        "--read-only",
        action="store_true",
        help="refuse write, remove, lifetime, keep and send",
    )
    args = parser.parse_args()
    if not (sys.stdin.isatty() and sys.stdout.isatty()):
        sys.exit("velis-tui needs an interactive terminal")
    locale.setlocale(locale.LC_ALL, "")
    if "utf" not in locale.getpreferredencoding(False).lower():
        sys.exit("velis-tui needs a UTF-8 locale (try LANG=C.UTF-8)")
    asyncio.run(_run(args.host, args.port, args.read_only))
