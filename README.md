<img width="300" height="100" alt="logo" src="https://github.com/user-attachments/assets/b047dd5e-2c8d-4eae-a8c4-516d81900fea" />


Velis is a lightweight, thread-safe in-memory store with WAL crash recovery, pub-sub, TTL expiration, pluggable codecs and a protocol selector layer.

> **Status: early development.** The codec system, configuration and logging are in place. The store, WAL, TTL, pub-sub and network layer are planned and described under [Roadmap](#roadmap); they are not implemented yet.

---

## What works today

| Area | Description |
| --- | --- |
| **Codecs** | A small `Codec` protocol, composable `CodecChain`s, and a registry that exposes a chain as a Python text codec. |
| **Settings** | Frozen, validated `Settings` loaded from `VELIS_*` environment variables. |
| **Logging** | Console logging in development, rotating file logging in production. |

---

## Installation

Velis uses [uv](https://docs.astral.sh/uv/) and a `src` layout.

```bash
git clone <repo-url>
cd velis
uv sync
```

See `requires-python` in `pyproject.toml` for the supported Python version.

---

## Running

```bash
uv run velis
```

For now this only loads settings and configures logging. The server arrives with the network layer.

---

## Configuration

Settings are read from environment variables. Invalid values (an unknown log level, a port outside 0–65535) raise `ValueError` at startup.

| Variable | Default | Description |
| --- | --- | --- |
| `VELIS_LOG_LEVEL` | `INFO` | Log level for the `velis` logger. |
| `VELIS_ENV` | _(unset)_ | Set to `production` to log to a file instead of the console. |
| `VELIS_LOG_FILE` | `/var/log/velis/velis.log` | Log file path, used only in production. |
| `VELIS_HOST` | `127.0.0.1` | Host for the network layer. |
| `VELIS_PORT` | `9000` | Port for the network layer. |

In production, logs go to a rotating file (5 MB per file, 5 backups) and parent directories are created if missing.

---

## Codecs

A codec is any object with a `name` and `encode` / `decode` methods that work on `bytes`:

```python
class Codec(Protocol):
    @property
    def name(self) -> str: ...
    def encode(self, data: bytes) -> bytes: ...
    def decode(self, data: bytes) -> bytes: ...
```

Codecs are combined into a `CodecChain` with `|`. Encoding runs the codecs left to right, and decoding runs them right to left. The chain's name is the codec names joined with `_`.

```python
import base64
import zlib

from velis.codec import CodecChain, register


class Deflate:
    name = "deflate"

    def encode(self, data: bytes) -> bytes:
        return zlib.compress(data)

    def decode(self, data: bytes) -> bytes:
        return zlib.decompress(data)


class B64:
    name = "b64"

    def encode(self, data: bytes) -> bytes:
        return base64.b64encode(data)

    def decode(self, data: bytes) -> bytes:
        return base64.b64decode(data)


chain = CodecChain() | Deflate() | B64()
chain.name  # "deflate_b64"
chain.decode(chain.encode(b"hello"))  # b"hello"
```

### Registering a chain as a Python codec

`register` makes a chain available to the standard `str.encode` and `bytes.decode` under the chain's name. Text is encoded with the given text encoding (default `utf-8`) before the chain runs, and decoded after it on the way back.

```python
register(chain)  # or register(chain, encoding="latin-1")

blob = "hello".encode("deflate_b64")
blob.decode("deflate_b64")  # "hello"
```

Notes:

* Registering an empty chain, or a chain whose name is already registered, raises `ValueError`.
* Avoid chain names that match built-in Python codecs (for example `zlib` or `base64`); the built-in codec is found first.
* When a codec fails, the original exception is re-raised with a note saying which codec and chain it happened in.

---

## Project layout

```
src/velis/
  __main__.py      # entry point: loads settings, configures logging
  settings.py      # Settings dataclass, loaded from VELIS_* env vars
  log.py           # logging configuration
  codec/
    chain.py       # Codec protocol and CodecChain
    registry.py    # register chains as Python text codecs
  net/             # client, framing, server (placeholders)
tests/
  codec/           # unittest tests for chain and registry
```

---

## Development

Tests use the standard library's `unittest`.

```bash
make test
```

This runs `uv run python -m unittest discover -s tests/codec -v`.

---

## Roadmap

Planned, in rough order:

* **In-memory store:** a thread-safe key-value store guarded by locks.
* **WAL persistence:** operations (`SET`, `DELETE`, `EXPIRE`) are appended to a write-ahead log before memory is changed, and the log is replayed on startup. Compaction rewrites the log to keep only live keys.
* **TTL expiration:** per-key expiry with lazy eviction on lookup, plus background pruning.
* **Pub-sub:** subscriptions to key mutations.
* **Network layer:** framing, server and client under `velis.net`.
* **Protocol selectors:** parsing client commands and routing them to store operations.
* **Built-in codecs:** ready-made codecs to compose with the existing chain.
