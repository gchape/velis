# Enterprise and integration pattern review

Reviewed against the current Velis working tree on 2026-11-10. The assessment below records the pre-implementation findings; the status table
below describes the changes subsequently implemented.

The sources are Fowler's _Patterns of Enterprise Application Architecture_ catalog
and subsequent articles, together with _Enterprise Integration Patterns_ by
Gregor Hohpe and Bobby Woolf, which has a foreword by Fowler. These are related
catalogs with different scopes. [Publisher's EIP listing](https://www.informit.com/store/enterprise-integration-patterns-designing-building-9780133065091).

The recommendations below are judgments about Velis, not prescriptions made by
those authors about this project. Velis is a single-process, volatile store and
pub-sub server. It has no database mapping, durable message delivery, distributed
transactions or server replication today.

## Implementation status

| Recommendation | Result |
| --- | --- |
| Submission as service boundary | Implemented: explicit admission outcomes and execution policy recheck |
| Narrow presentation dependencies | Implemented: SessionModel, sequence-based selection and isolated operation handlers |
| Typed client use cases | Implemented: read_entry and remaining_many, including per-key failures |
| Queued work expiration | Implemented: a shared ten-second queue/execution budget and explicit stale-work feedback |
| Messaging Bridge and replay | Optional future requirement; no delivery guarantees changed |

Commands now use READ/WRITE/REMOVE/LIFETIME/REMAINING/KEEP/WATCH/UNWATCH/SEND
on the wire and matching lowercase names in the TUI and Python client. Historical
code descriptions below use the vocabulary present during the original assessment.

## Highest-value changes

### 1. Make submission the application boundary

**Priority: high. Small change, measurable correctness benefit.**

Fowler's catalog describes a [Service Layer](https://martinfowler.com/eaaCatalog/serviceLayer.html)
as the boundary that coordinates available application operations. Velis already
has a server boundary in `Engine.execute`; it needs no extra service wrapper.
The actionable gap is in the terminal application:

- [Tui._submit](../src/velis/tui/ui.py) checks read-only mode and command metadata.
- [Workspace.submit](../src/velis/tui/workspace.py) checks connectivity and capacity,
  but accepts a write in read-only mode and accepts an unknown command name.
- [Operations.execute](../src/velis/tui/operations.py) indexes its handler mapping;
  an unknown name becomes a `KeyError`, escaping normal operation error handling.

A local probe with a mock Client confirmed both requests are accepted into the
queue. No network requests were sent. Normal keyboard submission is protected;
this is a reusable application-boundary defect, not a server authorization issue.

**Proposal:** make Workspace submission own command admission, read-only policy
and queue limits. Return an explicit outcome such as accepted, disconnected,
full, read-only or invalid. Tui displays the outcome and follows the selected key
only after acceptance. Keep the same command metadata for hints and policy; do
not build a second registry. If read-only can change while requests wait, recheck
it before execution or make that setting immutable.

**Verification:** direct submissions cannot bypass read-only mode; invalid/local
commands cannot enter the network queue; valid requests retain order; refresh
coalescing and existing keyboard feedback remain unchanged.

### 2. Reduce presentation dependencies further

**Priority: medium. Small change first; larger model extraction only if useful.**

The existing `ViewState`, `Entry` and `Stats` follow the intent of
[Presentation Model](https://martinfowler.com/eaaDev/PresentationModel.html):
state can be exercised without terminal controls. However,
[ViewState.selected_entry](../src/velis/tui/state.py) accepts an entire Workspace,
and Operations receives the scheduler object to access client, state and logging.
The previous extraction reduced class size more than it reduced dependency breadth.

**Proposal:** first pass a sequence of entries to selection logic, rather than a
Workspace. If more views or consumers appear, introduce one cohesive session model
for entries, channels and counters; inject that model, Client and a logging callback
into Operations. Keep tasks, queues, timeouts and client lifetime in Workspace.
Avoid a protocol that simply copies every Workspace attribute.

**Verification:** selection and operation-result application can be tested without
constructing the scheduler; preview memory limits, tracking eviction and counters
remain unchanged. This extraction is worthwhile only if it removes more coupling
than its forwarding code adds.

### 3. Preserve the gateway; add typed bulk use cases only when justified

**Priority: medium/conditional. No additional gateway class needed.**

A [Gateway](https://martinfowler.com/articles/gateway-pattern.html) isolates an
external API behind an interface suited to its callers. EIP's
[Messaging Gateway](https://www.enterpriseintegrationpatterns.com/patterns/messaging/MessagingGateway.html)
applies the same idea to message exchange.

[Client](../src/velis/net/client.py) already serves this role; Session owns
transport exchange, and `replies.py` owns shared interpretation. Preserve those
boundaries. `Operations._get` and `_refresh` intentionally use the lower-level
batch API and therefore still know GET/TTL message types.

**Possible enhancement:** if another consumer needs them, add typed Client helpers
for reading value-plus-TTL or fetching TTLs in bulk. Keep `batch()` as the explicit
raw reply API. Define partial-error behavior before adding a bulk helper; a refresh
should not silently hide errors after the first failed item.

A value-plus-TTL result must not claim an atomic snapshot: these remain separate
commands, TTL may advance, and batches can span execution turns. Do not introduce
a new server command or wire change solely to hide two message constructors.

### 4. Consider expiry for queued work, separately from cache TTL

**Priority: conditional. Useful if stale queued commands are a UX problem.**

[Message Expiration](https://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageExpiration.html)
limits how long queued work remains useful. `Workspace._perform` starts its
10-second timeout only after dequeueing. With 32 queued operations, time spent
waiting can be much longer than that timeout. `Store.expire` controls cached data;
it does not expire a request waiting in the TUI.

**Proposal:** if the product needs an end-to-end deadline, attach a monotonic
submission deadline and reject expired work before sending. Make this policy
visible, especially for writes. Retain the existing single pending refresh rather
than adding a deduplication framework. A deadline cannot undo a sent command.

**Verification:** expired requests never reach Client; queue accounting and refresh
flags are released; cancellation after transmission retains the existing semantics.

## Useful patterns already present

| Pattern                                                                                                                     | Current application                                                                                         | Recommendation                                                                                                                                                          |
| --------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Service Layer                                                                                                               | `Engine.execute` coordinates Store and Broker                                                               | Keep one boundary; avoid a wrapper per operation                                                                                                                        |
| [Transaction Script](https://martinfowler.com/eaaCatalog/transactionScript.html)                                            | Short operation handlers coordinate straightforward actions                                                 | Retain procedural handlers; the pattern name does not confer transaction atomicity                                                                                      |
| [Separated Interface](https://martinfowler.com/eaaCatalog/separatedInterface.html)                                          | Core Subscriber is implemented by the network connection; Clock and Session provide substitutable contracts | Preserve dependency direction. Session currently shares a module with TCPSession; physically separate it when a second transport or import constraint makes that useful |
| [Data Transfer Object](https://martinfowler.com/eaaCatalog/dataTransferObject.html)                                         | Immutable command/reply records cross the client/server boundary through codecs                             | Preserve explicit records and schemas; avoid generic dictionaries or duplicate DTO hierarchies                                                                          |
| [Request-Reply](https://www.enterpriseintegrationpatterns.com/patterns/messaging/RequestReply.html)                         | TCPSession matches replies to requests on one duplex TCP connection                                         | Preserve ordering and cancellation tests; this is not a durable two-queue implementation                                                                                |
| [Publish-Subscribe Channel](https://www.enterpriseintegrationpatterns.com/patterns/messaging/PublishSubscribeChannel.html)  | Broker fans out to current channel members                                                                  | Preserve at-most-once semantics and isolation of overloaded subscribers                                                                                                 |
| [Message Router](https://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageRouter.html), limited analogy      | TCPSession routes pushes to a buffer and replies to pending slots                                           | Keep the small branch; this does not require a general routing framework                                                                                                |
| [Pipes and Filters](https://www.enterpriseintegrationpatterns.com/patterns/messaging/PipesAndFilters.html), limited analogy | Framing → decoding → dispatch → encoding has distinct stages                                                | Keep synchronous function composition. These are not independent channel-connected workers; introducing per-stage queues would complicate ordering                      |

## Patterns for explicit future requirements

**Messaging Bridge — applicable if replication becomes a requirement.** A
[Messaging Bridge](https://www.enterpriseintegrationpatterns.com/patterns/messaging/MessagingBridge.html)
connects channels across messaging systems. A Velis replication adapter should
forward selected publications while leaving Store and eviction untouched. Start
with an explicit one-way, acyclic topology. Bidirectional or mesh forwarding
requires an origin/message identity and a loop-prevention policy. The current
client API subscribes to named channels, so an all-channel bridge would need a new
broker integration seam; wildcard subscription does not exist today. Bound peer
queues and define what happens on disconnect. Replication alone adds no durability.

**Idempotent Receiver — when replay or retries are introduced.**
[Idempotent Receiver](https://www.enterpriseintegrationpatterns.com/patterns/messaging/IdempotentReceiver.html)
addresses duplicate delivery. Velis currently does not retry publications.
`PUBLISH` may duplicate application effects when replayed; repeating relative
`EXPIRE` extends the deadline; even SET can clear a newer TTL or overwrite a newer
value. Automatic retry of every command is therefore unsuitable. Deduplication
would require stable operation IDs, defined retention/replay windows and bounded
storage. In-memory deduplication cannot survive a process restart.

**Correlation Identifier — when replies can arrive out of order.** EIP's
[Correlation Identifier](https://www.enterpriseintegrationpatterns.com/patterns/messaging/CorrelationIdentifier.html)
is an explicit request identity carried by replies. Current FIFO correlation in
[PendingReplies](../src/velis/net/pending.py) is sufficient because server replies
remain ordered and pushes are routed separately. Cancelled sent slots intentionally
remain until their replies arrive. IDs become useful for unordered execution or
conversations spanning transports; adding them now costs a wire migration and
pending-map complexity. TCP byte ordering alone would not guarantee reply order
if server execution later became concurrent.

## Patterns that would add complexity today

| Pattern                                                                                                                                 | Why it does not simplify current Velis                                                                             | Reconsider when                                                                  |
| --------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------- |
| [Repository](https://martinfowler.com/eaaCatalog/repository.html), Data Mapper                                                          | Store owns in-memory values and their invariants; there is no domain-to-database mapping or duplicated query layer | Persistent domain objects and substantial query/mapping logic exist              |
| [Unit of Work](https://martinfowler.com/eaaCatalog/unitOfWork.html)                                                                     | No change-tracked persistence transaction exists; Client.batch is a transport optimization                         | A real commit/rollback persistence model is required                             |
| [CQRS](https://martinfowler.com/bliki/CQRS.html)                                                                                        | Separate read/write models add synchronization costs. GET also updates statistics and eviction state               | Read and write requirements diverge enough to justify distinct models            |
| [Guaranteed Delivery](https://www.enterpriseintegrationpatterns.com/patterns/messaging/GuaranteedMessaging.html), durable subscriptions | Current messages are volatile, unacknowledged and not replayed                                                     | Persistence, acknowledgement and recovery become product requirements            |
| Aggregator, Resequencer, Process Manager                                                                                                | No multi-party workflow or unordered response stream needs coordination                                            | Fan-out requests, independent reply streams or long-lived workflows appear       |
| Global Registry/service locator                                                                                                         | Explicit constructors and `app.serve` already expose ownership                                                     | Prefer explicit composition unless a concrete integration constraint prevents it |

The TUI's cached previews are session observations, not a CQRS read database.
Likewise, the class named Broker is a small subscription dispatcher, not an
enterprise integration hub. Matching names alone is not evidence that a broader
pattern implementation is needed.

## Suggested implementation order

1. Centralize submission policy and add direct-entry regression tests.
2. Narrow ViewState inputs; measure whether a separate session model reduces total coupling.
3. Add typed bulk Client helpers only with a clear reuse case and error contract.
4. Decide queue-deadline semantics before introducing Message Expiration.
5. If replication becomes a requirement, design it as a separate bridge.

The original research used source review, code tracing and an isolated submission
probe. Implemented changes are covered by admission, deadline, model, partial-failure,
protocol, client and terminal behavior tests; see README for the verification commands.
