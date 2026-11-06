# Velis

Velis is a lightweight, thread-safe in-memory store featuring a Write-Ahead Log (WAL) for crash recovery, real-time pub-sub event broadcasting, TTL key expiration, pluggable codecs, and a protocol selector layer.

---

## Features

* **WAL Persistence:** Transactions are written to disk before RAM mutations to guarantee zero data loss on restart.
* **TTL Expiry:** Built-in expiration support with lazy eviction.
* **Pub-Sub Broker:** Key-level mutation subscriptions (`SET`, `DELETE`, `EXPIRE`).
* **Pluggable Codecs:** Modular serialization support (JSON, binary).
* **Protocol Selectors:** Command parsing and state routing for client requests.
* **Thread Safety:** Fine-grained concurrency control via `threading.Lock`.

---

## Installation

```bash
pip install .

```

---

## Quick Start

```python
from velis import Velis, JSONCodec

# Initialize Velis with a WAL file and codec
db = Velis(log_file="app.velis", codec=JSONCodec())

# 1. Subscribe to key updates
db.subscribe("user:100", lambda k, v, act: print(f"Event -> {act}: {k} = {v}"))

# 2. Set a value with a 60-second TTL
db.set("user:100", {"name": "Alice", "role": "admin"}, ttl=60)

# 3. Fetch from memory
print("Retrieved:", db.get("user:100"))

# 4. Simulate a restart recovery
recovered_db = Velis(log_file="app.velis", codec=JSONCodec())
print("Recovered Data:", recovered_db.get("user:100"))
```

---

## Architecture

* **Write-Ahead Log:** Appends serialized operations (`SET`, `DELETE`, `EXPIRE`) to disk before memory updates. On startup, the log replays to restore state.
* **Codecs & Selectors:** Encodes/decodes data formats and routes incoming protocol commands to the appropriate store operations.
* **TTL Management:** Evaluates expirations during lookups and background pruning routines.

---

## API Reference

* `Velis(log_file="velis.log", codec=None)`: Initializes the store and executes WAL recovery.
* `set(key, value, ttl=None)`: Writes to WAL, updates memory, sets TTL, and broadcasts events.
* `get(key)`: Retrieves the value if active and unexpired; otherwise returns `None`.
* `delete(key)`: Removes the key, logs the deletion, and notifies subscribers.
* `subscribe(key, callback)`: Registers a listener function `callback(key, value, action)`.
* `compact()`: Rewrites the WAL file to retain only active live keys.