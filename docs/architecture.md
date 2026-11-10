# Design responsibilities

Velis uses composition and narrow interfaces to keep the synchronous core separate
from transport, application lifecycle, and terminal presentation. Each abstraction
has a concrete owner and a reason to change.

For a broader pattern-by-pattern assessment and proposed next steps, see the
[enterprise and integration pattern review](enterprise-patterns.md).

| Boundary               | Responsibility                                | Design choice                                                                        |
| ---------------------- | --------------------------------------------- | ------------------------------------------------------------------------------------ |
| Store → Eviction       | Select capacity victims without owning values | Strategy through a structural protocol; LRU, LFU and S3-FIFO share one contract      |
| Store → Clock          | Sample monotonic time                         | Injected port; deterministic clocks require no inheritance                           |
| Engine → Store/Broker  | Execute typed commands                        | Explicit exhaustive dispatch over immutable command data                             |
| Broker → Subscriber    | Accept a publication synchronously            | Small delivery port; sockets remain outside the core                                 |
| Client → Session       | Exchange requests and publications            | Transport port with a TCP adapter                                                    |
| Connection → dispatch  | Decode and execute one bounded request prefix | Delegate a synchronous operation; connection owns scheduling and socket flow control |
| Client → replies   | Interpret READ, REMAINING and server errors          | Shared pure functions keep single calls and batch consumers consistent               |
| app.serve              | Construct and release runtime resources       | Composition root and structured resource ownership                                   |
| Keyboard → CommandEditor    | Edit drafts and browse history                | Delegation; editor owns history, controller presents admission outcomes                          |
| Workspace → Operations | Apply command results to session state        | Scheduler owns timeouts/errors; handlers own successful operation effects            |
| Renderer → presenters  | Draw one terminal frame                       | Composition of layout, pane, overlay and chrome presenters                           |
| Presenters → Surface   | Escape, clip and write text                   | Terminal adapter owns curses calls and theme fallback                                |

## Terminal behavior

The header prioritizes connection and read-only status. A stable key list sits
beside a value preview and recent activity. Compact value metadata leaves room
for content even at 60 columns by 14 rows. Text previews wrap by terminal cells;
binary previews select a hex row width that fits. Navigating keys never fetches
values automatically: Enter explicitly reads and updates eviction usage.

Help and statistics are scrollable documents. Arrows or j/k scroll, PgUp/PgDn
page, Home/End or g/G jump, and Esc returns. Command mode shows usage for the
current verb; read-only mode labels disabled writes and offers a read command in
the empty state. NO_COLOR disables all color pairs, while selection and severity
remain distinguishable through attributes and text.

Presentation state lives outside curses. The renderer coordinates small
presenters rather than implementing every pane. Stateless presentation uses
functions; stateful editing and value scrolling use collaborators. This keeps
layout and keyboard behavior testable without a terminal.

## Why these patterns

[Martin's single-responsibility guidance](https://blog.cleancoder.com/uncle-bob/2014/05/08/SingleReponsibilityPrinciple.html)
provides the criterion for splitting socket flow control from command dispatch,
and queue lifecycle from operation effects: these responsibilities change for
different reasons. [Fowler's presentation model](https://martinfowler.com/eaaDev/PresentationModel.html)
informs keeping display state accessible independently of the terminal controls.

[Fowler's dependency-injection discussion](https://martinfowler.com/articles/injection.html)
informs the explicit composition root and constructor-supplied collaborators.
[Python's structural protocols](https://typing.python.org/en/latest/reference/protocols.html)
allow implementations and test doubles to satisfy small interfaces without shared
base classes. [Textual's layout guide](https://textual.textualize.io/how-to/design-a-layout/)
provides a useful terminal layout reference for persistent header/footer regions
and separately composed content areas; Velis continues to use standard-library
curses.

The closed command union remains exhaustive pattern matching. A registry or
class-per-command hierarchy would make coverage harder to check here. There is
no plugin requirement that warrants an abstract factory, no reversible server
transaction to justify undo commands, and no need for a global event bus where a
single redraw callback already expresses ownership. These can be reconsidered
when requirements change.

## Verification and limits

Behavioral tests cover keyboard editing/history, scrolling to the end of help,
small-screen value space, read-only guidance, monochrome fallback, Unicode
wrapping, output escaping and screen bounds. Existing core and network tests
cover dispatch ordering, malformed frames, backpressure and cleanup. Ruff enforces
method complexity and statement limits; strict Pyright and ty check interfaces.

The terminal still requires at least 60 columns and 14 rows. Unicode cell width
is an approximation based on the standard library; terminal-specific emoji and
grapheme shaping may vary. Value previews and statistics describe this client session. LIST discovers
pages of server key names; it is weakly consistent under concurrent mutation. The ownership model remains one asyncio event-loop
thread; these refactors do not introduce thread safety or change wire framing.

## Implemented application boundary and command migration

The terminal and wire now use READ, WRITE, REMOVE, LIFETIME, REMAINING, KEEP,
WATCH, UNWATCH and SEND. The Python client exposes matching lowercase methods;
terminal-only commands manage local state. Legacy verbs and hex writing are removed.

Workspace admission owns command validation, connectivity, read-only policy and
queue capacity, and returns an explicit Submission outcome. Execution rechecks
mutable policy. Each QueuedRequest carries one monotonic deadline covering both
queue wait and network execution; expired work is discarded before transmission.

SessionModel owns bounded previews, watched channels and counters. Operations
receives that model, Client and a log callback, without access to the scheduler.
ViewState consumes entry sequences and implements local name filtering.
KeyboardController delegates draft editing and filtering; Tui owns terminal tasks.

Typed Client.read_entry and remaining_many hide reply interpretation. Sync keeps
successful updates after per-key errors and reports failures. These helpers preserve
the existing non-transactional semantics and bounded transport windows.

Metadata commands PING, EXISTS, SIZE and LIST support discovery without value
reads. Paginated names use a bounded tuple codec. Listing snapshots at most 1,000
candidates per call; cursor traversal can still take linear time. DiscoveryOperations
handles their terminal feedback separately from value-changing operations.
