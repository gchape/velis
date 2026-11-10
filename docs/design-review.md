# Velis design review

This document consolidates three review passes, all dated 2026-11-10:

1. **Performance and simplicity.** Compared against the working tree as it stood when the review started, not git HEAD.
2. **Architecture.** Reviewed against the implementation after pass 1.
3. **Library and composability.** Compared against the completed tree from pass 1.

Benchmark results, sample ranges, workload settings and environment details are inline in the [README](../README.md#benchmarks). The repository does not retain raw result files. Conclusions here are engineering judgments about Velis, not guarantees supplied by a pattern name or a library's own claims.

## Summary

- **Boundaries kept:** core/network, eviction strategy, client/session, codec tables, runtime scope, and the small `ExpiryIndex` and `Memberships` helpers.
- **Removed as unneeded:** the socket-tuning subsystem (an intentional API change), the one-use `Memory` wrapper, the passive flow-control wrapper and several forwarding methods.
- **Fixed:** unbounded shutdown on a peer that stops reading, numeric validation gaps, core imports that pulled in application configuration, and a monolithic configuration method.
- **Faster:** server-side buffer decoding, single-join record encoding and skipping unchanged TTL deadlines.
- **Libraries:** none added. uvloop, sortedcontainers and heapdict were tried and deferred or rejected. The others were assessed from their APIs and found not to replace a substantial subsystem.
- **Open:** tail latency under maintenance, pub-sub fan-out latency, total batch memory, eviction workload fit, and narrow performance evidence (see [Remaining limitations](#remaining-limitations-and-priorities)).

## Criteria and sources

An abstraction should make today's code easier to understand. Extension points are introduced when there is a concrete substitution need. The table lists each source and how it was applied.

| Source | Application to Velis |
| --- | --- |
| [YAGNI — Fowler](https://martinfowler.com/bliki/Yagni.html) | Separates useful refactoring from speculative extensibility. Removed the socket-policy subsystem and the one-use storage wrapper. |
| [Modular design — Ousterhout](https://web.stanford.edu/~ouster/cgi-bin/cs190-winter18/lecture.php?topic=modularDesign) | Prefers small interfaces hiding substantial implementation knowledge. Pass-through layers and leaked details are warning signs. |
| [Ports and adapters — Cockburn](https://alistair.cockburn.us/hexagonal-architecture) | Run and test behavior independently of external devices. Retain narrow `Session` and `Subscriber` boundaries. Core behavior runs without network I/O. |
| [Composition root — Seemann](https://blog.ploeh.dk/2011/07/28/CompositionRoot/) | Assemble dependencies near the entry point (`build_engine()`, `serve()`). No container or service locator is needed. |
| [Single responsibility — Martin](https://blog.cleancoder.com/uncle-bob/2014/05/08/SingleReponsibilityPrinciple.html) | Group code by reasons for change and coherent invariants. Splitting every method into a class would obscure them. |
| [Bounded pipelining — Redis](https://redis.io/docs/latest/manual/pipelining/) | Retain count and byte windows. Batching amortizes round trips and writes, while pending replies consume memory. No universal optimum follows from one localhost benchmark. |
| [Buffered protocols — Python](https://docs.python.org/3/library/asyncio-protocol.html#buffered-streaming-protocols) | Buffer ownership can reduce copying. Direct decoding from the existing bytearray captures a useful gain without replacing the transport with `BufferedProtocol`. |
| [Lazy priority-queue deletion — Python](https://docs.python.org/3/library/heapq.html#priority-queue-implementation-notes) | Keep a deadline map plus heap. Avoid duplicate work and retain compaction. Amortized bounds are not per-turn latency bounds. |
| [S3-FIFO — authors](https://s3fifo.com/blog/2023/08/01/fifo-queues-are-all-you-need-for-cache-eviction/) | Retain cheap hit counters and deferred promotion. Velis uses resident-count-based queue targets, so the paper's results are not direct Velis performance claims. |

These are choices informed by the sources, not requirements to add every named pattern.

## Boundaries that earn their place

| Boundary | Evidence in this code | Decision |
| --- | --- | --- |
| Core / network | `Store`, `Broker` and `Engine` run without sockets. Their tests use a manual clock and recording subscribers. | Keep. Network framing and flow control belong outside the core. |
| Eviction strategy | LRU, LFU and S3-FIFO are real implementations behind the same operations. | Keep. `Store` owns consistency. Policies choose victims. |
| Client / session | `Client` translates typed results. `TCPSession` owns ordering, cancellation, backpressure and message buffering. | Keep. They describe different responsibilities, even with one production transport. |
| Codec tables | One declaration supplies encoding, decoding, coverage checks and maximum frame sizes. | Keep. Startup reflection has a real purpose. Extending it into a general serialization framework would need evidence. |
| Runtime scope | The CLI loads environment and owns signals. `serve()` assembles and closes runtime resources. | Keep explicit construction and async context managers. |
| Small helpers | `ExpiryIndex` owns timer heaps. `Memberships` owns two synchronized indexes. | Keep these independent algorithms. Fold the one-use `Memory` wrapper into `Store` (see below). |

`Session` and `Subscriber` are interfaces, and their concrete implementations are the adapters. Frozen command records are data passed to an interpreter, so no separate command-handler class per operation is needed. The existing `match` dispatch is sufficient. Additional repositories, service layers, factory hierarchies or state-class hierarchies have no demonstrated need here.

## Changes made

### Performance and simplicity

- **Socket policy removed.** Deleted `net/sockets.py`, custom keepalive tuning, its `Keepalive` configuration value and four environment settings. Asyncio and OS socket defaults now apply, and Velis no longer enables TCP keepalive. Request timeouts and bounded shutdown still apply separately. See [Defaults and socket policy](#defaults-and-socket-policy) for the migration.
- **`Memory` folded into `Store`.** Values and their recorded byte sizes now have one owner. This removes forwarding calls and a redundant dictionary deletion during writes. Expiry scheduling and eviction remain separate.
- **Connection simplified.** Removed the passive flow-control wrapper and several single-use forwarding methods. Three flow-control flags live directly on `Connection`. Transport callbacks, bounded execution and cleanup remain distinct methods. The client/session and core/network boundaries remain.
- **Decoding from buffer offsets.** Server commands are decoded in place and only retained fields are copied into owned values. A view never escapes decoding or pins the mutable receive buffer. The body-copying `iter_frames()` API is still available. Field limits, frame boundaries, ordering and malformed-input behavior are unchanged.
- **Single-join encoding.** A record is encoded with one join over its tag and fields, avoiding a generator and an additional whole-body concatenation.
- **Unchanged TTL deadlines skipped.** Readiness checks stop at future heap heads instead of scanning stale entries that cannot conceal a due timer.

The wire format and cache semantics are unchanged. Regression coverage includes all valid records inside larger buffers, every truncated record boundary, payload ownership after buffer mutation, TTL rescheduling and store byte accounting.

### Shutdown fix

Shutdown previously called `close_clients()` and then waited without a deadline. [Asyncio transport close](https://docs.python.org/3.15/library/asyncio-protocol.html#asyncio.BaseTransport.close) flushes buffered output before reporting disconnection, so a peer that stops reading could keep cleanup waiting. Cancelling server cleanup also cleared the listener reference without aborting its remaining clients.

Both server and session now allow a configurable five-second flush grace period, then abort stalled transports. Cancellation also aborts. Session cleanup preserves the writer's shared close waiter instead of cancelling it during timeout. Tests cover blocked and cancelled cleanup, including a real TCP subscriber that stops reading. Five seconds is a shutdown policy choice, not a throughput optimum.

### Defaults and socket policy

The existing 64-request window and 256 KiB batch target are supported as reasonable starting points by the local benchmark, not as optimal values for every workload. The store's key and byte limits account for different costs. Pub-sub buffers are per connection, so their sum is not a process-wide memory ceiling.

Connections use asyncio and OS defaults, and asyncio already [enables TCP_NODELAY](https://docs.python.org/3.15/library/asyncio-eventloop.html#asyncio.loop.create_connection). **This is an intentional API change.** Remove `Keepalive` imports and `keepalive=` arguments from embedded callers. Old `VELIS_TCP_KEEP*` variables no longer configure anything.

### Strict review fixes

1. **Invalid resource counts passed construction.** Fractional request windows could reach integer-only iteration during batching. Ports, counts, byte budgets, subscription limits and sweep intervals now require actual integers, excluding booleans. Fractional finite shutdown durations remain valid.
2. **Invalid numbers escaped the intended error boundary.** Fractional TTLs could enter the store, and fractional wire integers could reach `struct.pack` and raise its low-level error. Store TTLs now reject them with `CommandError` without mutation. Unsigned wire fields reject them with `ProtocolError` before packing.
3. **S3-FIFO accepted non-finite ghost ratios.** NaN and infinity could fail later during ghost trimming. Construction now rejects them.
4. **Core imports pulled in application configuration.** Immutable defaults and limits now live in `settings.py`, and environment loading remains in `config.py`. Core imports no network or environment-loading module. Existing imports from `velis.config` remain available as explicit re-exports.
5. **One configuration method assembled every subsystem.** `Config.from_env()` went from 57 to 19 lines. A private environment reader parses the mapping and constructs each settings group in a short method, reusing dataclass defaults.
6. **The TCP session also implemented the reply queue algorithm.** `PendingReplies` now owns capacity reservation, FIFO resolution, cancellation tombstones and failing pending futures. It performs no encoding or I/O. `TCPSession` composes it with transport reads/writes and the bounded push buffer. The uninterrupted reservation-to-write ordering rule remains explicit and regression-tested.

### Responsibility and size

Single responsibility is applied around reasons for change and coherent invariants:

- `Store` owns value, byte-budget and lifecycle consistency. `ExpiryIndex` owns scheduling. The eviction strategies own victim selection.
- `Connection` owns server framing, backpressure and transport lifetime. Its bounded batch executor is separate from scheduling and cleanup. It does not own cache internals or client reply bookkeeping.
- `PendingReplies` owns FIFO reply slots, capacity and cancellation tombstones. `TCPSession` composes it with transport I/O and client lifetime, and its private message buffer owns push count/byte accounting. `Client` owns typed API interpretation.
- `TCPServer` owns listener admission. `serve()` owns resource lifetimes and assembles dependencies explicitly.

The narrow `Clock`, `Eviction`, `Subscriber` and `Session` interfaces give real substitution points and test seams. There is no service locator, container, generic repository or event bus, and adding one would not remove current work. Pure limits live in `settings.py`, and configuration loading composes short per-component parsers.

`TCPSession` is now 206 lines including documentation, down from 230, and its extracted reply component is 51 lines. This adds an independently understandable owner rather than reducing total source lines. `Config` is 38 lines and its environment reader is 51. The longest production function is 33 lines including signature and docstring. Line counts are review aids, not proof of good boundaries.

**Guardrails.** Ruff enforces production function limits of McCabe complexity 10, at most 12 branches and at most 25 statements, through the existing `make check` workflow. Test fixtures and benchmark drivers are excluded from these size rules but keep normal lint and formatting checks.

## Measurements

The [README benchmark section](../README.md#benchmarks) holds the TCP, encoding, dispatch and TTL results, including sample ranges and reproduction commands. Its tables distinguish the original performance comparison from the later composability run. These are machine-specific results, not production latency guarantees.

- **Profiling.** Tracing 96,000 small SET dispatches identified `Store.set()` and `Codec.decode()` as the main cumulative costs below dispatch. Profiling located the work, and the reported timing runs were unprofiled.
- **Isolation.** Isolated measurements use fresh state and a separate tracemalloc pass for incremental allocation peaks.
- **Composability run.** The matching TCP run used 20,000 operations, 2,000 warmups, three repeats, 64-byte values, batch and raw modes, and depths 1 and 64. Median changes ranged from -2.9% to +6.2%. That does not establish a material throughput gain or regression. Asyncio remained the loop for both completed runs.

All benchmarks run from one script, `benchmarks/bench.py`:

```sh
python benchmarks/bench.py tcp --mode batch raw --pipeline 1 64
python benchmarks/bench.py hot-paths
python benchmarks/bench.py expiry   # needs the research dependencies below
```

## Library evaluation

Velis stays dependency-free for this revision. Three candidates were installed into an isolated `/tmp` directory and exercised. The others were assessed from their documented APIs and the code they would replace. No package was added to the application's environment, dependency list or lockfile.

This is not a ban on libraries. A replacement must simplify the complete behavior we need, preserve its contracts, and have evidence for any claimed speedup. Library benchmark claims were not treated as measurements of Velis.

| Candidate | Potential benefit | Evidence and decision |
| --- | --- | --- |
| [uvloop 0.23.0](https://pypi.org/project/uvloop/0.23.0/) | Replace asyncio's scheduler and transports with a libuv implementation. | Installed on CPython 3.15.2. Running the async suite on its loop produced 25 errors across 183 tests, because `uvloop.loop.Server` lacks `close_clients()`. Inspection confirms `abort_clients()` is absent too. **Defer.** Preserving bounded shutdown would need explicit connection tracking or a compatible release. No uvloop throughput claim is made. |
| [sortedcontainers 2.4.0](https://grantjenks.com/docs/sortedcontainers/implementation.html) | Replace the lazy expiry heap with a sorted sequence, removing stale timers and custom compaction. | Benchmarked with a matching adapter. Rescheduling was about 4.3× slower and cancellation 2.8× slower, while stale-readiness checks were much faster. **Not the default.** Worth investigating for TTL-heavy latency requirements. |
| [heapdict 1.0.1](https://github.com/DanielStutzbach/heapdict) | An indexed priority queue supports update and removal without stale entries. | Matching adapter was about 17.5× slower for rescheduling and 15.7× slower for cancellation. **Reject** as a throughput improvement for this workload. |
| [msgspec](https://msgspec.dev/) | Fast records, serialization and validation. | Its built-in formats are JSON, MessagePack, YAML and TOML, not the current tagged binary format. Using it only for records still leaves custom field codecs and changes schema inspection. Strong candidate for a deliberate future wire-format migration, not a drop-in reduction. Not benchmarked. |
| [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) | Declarative environment parsing and validation across multiple sources. | Current configuration has one explicit mapping and three small settings groups. Preserving isolated mappings, flat environment names, strict integer contracts and domain settings still needs aliases/conversion or a public model migration. Revisit if file, secrets or nested-source requirements grow. Startup parsing is outside the hot path. Not benchmarked. |
| [attrs](https://www.attrs.org/en/stable/why.html) | Validators and converters on record fields. | Existing frozen, slotted dataclasses already generate construction and equality. The remaining checks are a few short shared validators, so it would change syntax rather than remove a subsystem. Not benchmarked. |
| [Construct](https://construct.readthedocs.io/en/latest/intro.html) | Declarative binary parsing and building could replace some field codec machinery. | Velis already declares layouts once. Character limits, pre-body length checks, domain records, frame limits and error translation still need adapters. No evidence of a performance gain, so do not replace a measured buffer parser speculatively. Not benchmarked. |
| [cachetools](https://cachetools.readthedocs.io/en/stable/) | Maintained cache policies and TTL collections. | Its size budget normally measures values through `getsizeof`. Velis combines key count, UTF-8 key/value bytes, mutable per-key TTLs, persistence, stats and selectable policies, so adapting would leave much of `Store` in place. Not benchmarked. |

### Expiry experiment

The [README expiry-library comparison](../README.md#expiry-library-comparison) contains the median timings, workload settings and reproduction commands for the current heap and the two adapters.

The indexed alternatives move work from later cleanup into each update or removal. That can be the right choice when latency under TTL churn matters more than update throughput. These synthetic runs do not establish production p99 latency or hard time bounds, and the SortedList implementation also has internal block maintenance.

Reproduce with the optional research dependencies installed in an isolated target:

```sh
uv pip install --python .venv/bin/python --target /tmp/velis-library-review sortedcontainers==2.4.0 heapdict==1.0.1
PYTHONPATH=/tmp/velis-library-review .venv/bin/python benchmarks/bench.py expiry
```

The `expiry` command contains the adapters, workloads and checks. Neither adapter is imported by application code, and their short methods are intentionally comparable to the production expiry interface.

## Validation

- **Pass 1:** Ruff lint and formatting, ty, strict Pyright and 183 tests under `python -X dev -W error`, including real TCP, cancellation, shutdown and generated store-state tests. Socket-tuning tests were removed with that feature, and existing transport behavior tests remain.
- **Pass 3:** 187 tests under development mode with warnings as errors, plus Ruff, ty and strict Pyright. Existing cancellation, concurrent batching, late reply, push overflow, backpressure and bounded-shutdown tests exercise the new composition. New tests cover numeric validation and public settings re-exports.

Passing tests and recognizable patterns are useful evidence, but they do not establish that every design decision is optimal.

## Remaining limitations and priorities

None of these is solved merely by splitting methods or adopting a pattern name.

1. **Tail latency under maintenance.** Sweep limits bound returned keys, not stale heap pops, occasional O(n) compaction, or the number of eviction victims needed to fit a large value. Lazy-heap cleanup can stall a turn. Measure competing-client p95/p99 during TTL and capacity pressure before choosing an indexed heap or incremental cleanup.
2. **Pub-sub fan-out.** `Broker.publish()` visits all subscribers synchronously, so one publication can exceed a normal command turn's latency. A 64-command window does not bound it. Measure fan-out and slow-subscriber behavior before introducing asynchronous delivery, which would change ordering and delivery-count semantics.
3. **Client cost and total batch memory.** `TCPSession.batch()` validates and retains all encoded commands before sending and uses one future per pending request. Window limits bound unanswered requests, not caller-owned inputs or returned results. Streaming would need an explicit contract for invalid commands and partially applied batches.
4. **Eviction workload fit.** LFU heap work, S3-FIFO promotion and ghost history, and LRU ordering have different costs. Compare hit rate and latency on representative traces before changing the default policy.
5. **Narrow performance evidence.** Current throughput reports cover one localhost connection. They do not establish p95/p99 latency, multi-connection fairness, TTL churn, eviction pressure or an optimal connection limit.

The next architectural work should follow these measurements.
