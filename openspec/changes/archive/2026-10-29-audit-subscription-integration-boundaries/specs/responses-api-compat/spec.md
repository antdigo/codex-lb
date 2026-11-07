## ADDED Requirements

### Requirement: Public stream normalization fences explicit foreign response owners
A public stream that accepted response A MUST reject an explicit envelope for a
different response before output backfill, synthetic rendering or forwarding.
It MUST emit an explicit terminal failure for A without foreign output or replay.
Native passthrough MUST retain its existing wire behavior.

#### Scenario: Foreign terminal after acceptance
- **GIVEN** response.created accepted A
- **WHEN** completed, incomplete or failed names B
- **THEN** no B text, items, arguments or successful terminal reaches the public consumer
- **AND** A receives an explicit terminal failure

#### Scenario: Locally generated failures preserve their cause after acceptance
- **GIVEN** a public stream accepted A and its in-process error wrapper catches a failure without an explicit response owner
- **WHEN** the wrapper generates a terminal failure
- **THEN** it MUST terminate A with the original error code, message, param and retry hint
- **AND** native transport-failure behavior MUST remain unchanged
- **AND** upstream JSON MUST NOT forge the in-process provenance of that failure

#### Scenario: Generated errors retain provenance through classic and retry layers
- **WHEN** classic core, service, retry or public normalization synthesizes a failure after acceptance
- **THEN** serialization MUST preserve trusted in-process provenance of locally added response IDs
- **AND** local IDs MUST NOT replace the accepted upstream owner in settlement or owner caches
- **AND** true foreign response IDs MUST NOT overwrite those caches before public rejection

#### Scenario: Public rejection or disconnect closes the owned service iterator
- **GIVEN** a public account stream passed through its startup probe and rendering wrappers
- **WHEN** ownership rejection, client disconnect or normal completion ends delivery
- **THEN** teardown MUST deterministically close the owned service iterator and settle its resources
- **AND** this MUST hold for an immediate first item and an outstanding startup probe, without relying on garbage collection
- **AND** active keepalive reads MUST be cancelled and joined before their child iterators are closed

#### Scenario: ASGI writer fails before or during streaming
- **WHEN** ASGI delivery is disconnected or cancelled during headers, a response event or a keepalive write
- **THEN** the response lifecycle MUST cancel and join outstanding startup and keepalive reads and close its owned service iterator
- **AND** cleanup MUST run even when the body generator never started, settle owned resources once and preserve the original delivery outcome and exception

#### Scenario: Classic service teardown closes its owned core iterator
- **GIVEN** classic streaming entered an owned upstream HTTP response or ephemeral transport context
- **WHEN** headers or a response-event write disconnects the client and API cleanup closes the service iterator
- **THEN** the service MUST also close its owned core iterator and upstream context before cleanup returns
- **AND** closure MUST preserve original terminal/usage outcomes and MUST NOT close a reusable sibling transport

#### Scenario: Exceptions after an accepted terminal preserve its settlement
- **GIVEN** an owned terminal was accepted with its usage and outcome
- **WHEN** continued iteration or transport context cleanup raises an exception
- **THEN** the accepted outcome, usage and request log MUST remain authoritative
- **AND** pre-terminal failures and intentional terminal failure control flow MUST retain their existing behavior

### Requirement: Public output lifecycle recovery handles owned snapshots consistently
A public first terminal with owned nonempty output MUST receive complete item and
content/tool lifecycles when no visible output was replayed. This MUST hold with
no prelude and with short or oversized discarded indexed preludes. Already
replayed legacy unindexed visible output MUST NOT be duplicated.
Owned terminal output items absent from an already streamed indexed lifecycle
MUST receive their missing lifecycle without replaying existing item content.

#### Scenario: Terminal contains tools after absent or discarded indexed prelude
- **WHEN** first completed or incomplete contains reasoning, message and function call after zero, one or sixty-five orphan indexed events
- **THEN** output items open before content/arguments and close before the terminal, with one created envelope
- **AND** consumers receive message/arguments once and no orphan output

#### Scenario: Terminal adds a message after an already streamed reasoning item
- **GIVEN** created A and an indexed reasoning item were accepted with their lifecycle
- **WHEN** A terminates with that reasoning item and a new message, refusal or tool item
- **THEN** new item/content/arguments lifecycles MUST open before deltas and close before the terminal
- **AND** existing reasoning, message and tool content MUST NOT be replayed
- **AND** the actual OpenAI SDK stream consumer MUST accept the complete sequence

#### Scenario: Terminal fills an already opened message
- **GIVEN** an indexed message was opened with empty content or fewer content parts
- **WHEN** its owned terminal contains previously absent text or refusal parts
- **THEN** missing content-part openings MUST precede synthetic deltas and their done events
- **AND** already visible text or tool arguments MUST NOT be replayed

#### Scenario: Owned item-done snapshot precedes the terminal
- **WHEN** an owned indexed output_item.done supplies message parts or tool arguments before any matching opening or delta events
- **THEN** missing item/content/argument lifecycles MUST precede that original done event
- **AND** that done event MUST be forwarded once and later terminal recovery MUST NOT duplicate its visible content

#### Scenario: Legacy reasoning lacks an output index
- **GIVEN** owned reasoning summary events have no output index before or after response creation
- **WHEN** an owned terminal supplies that reasoning and a previously absent message or tool
- **THEN** missing indexed item/content/argument openings MUST still be repaired for the SDK consumer
- **AND** previously visible reasoning MUST NOT be replayed or emitted as assistant text

### Requirement: Public item openings respect authoritative output positions
Public item/content/argument lifecycles MUST NOT reach the SDK at a sparse output
index before preceding item slots are available. Missing slots MUST come from
upstream announcements or owned snapshots; the gateway MUST NOT invent placeholder
items. Deferred output MUST preserve item IDs, original output/content indexes,
content and argument order within each item, and monotonic public sequence numbers.
Native passthrough MUST bypass this ordering state.

#### Scenario: Sparse owned item-done precedes the terminal
- **GIVEN** created A followed by an owned message or tool snapshot at index 1 without an index-0 opening
- **WHEN** A terminates with authoritative items 0 and 1
- **THEN** item 0 MUST open before item-1 content or argument events are emitted
- **AND** account and model-source SDK consumers MUST reach the final response with call IDs, ciphertext and visible content preserved once

#### Scenario: Earlier item arrives while later output is deferred
- **WHEN** upstream fills the missing earlier opening before the terminal
- **THEN** eligible deferred output MUST resume without waiting for the whole response
- **AND** valid interleaved streams whose preceding items already opened MUST continue immediately

#### Scenario: Deferred output is bounded and cleanup-safe
- **WHEN** a public stream needs to defer sparse output
- **THEN** it MUST retain at most 64 events and 16 MiB of serialized UTF-8 SSE data, without concatenating delta text or retaining an unbounded text buffer
- **AND** overflow or an unresolvable successful terminal MUST produce an explicit failure for the accepted owner
- **AND** an upstream failure MUST retain its original cause, while pending output is discarded
- **AND** cancellation or completion MUST free deferred state and close the owned input without a background drain task

### Requirement: Legacy text synthesis excludes reasoning content
Legacy text synthesis MUST emit assistant text only from message output, without
flattening reasoning into text. Reasoning fields and encrypted content MUST be
preserved in their original response fields.

#### Scenario: Partial reasoning precedes terminal
- **GIVEN** accepted reasoning output suppresses full terminal replay
- **WHEN** terminal reasoning has textual content and ciphertext
- **THEN** neither reasoning text nor ciphertext is emitted as assistant text
- **AND** terminal reasoning remains intact

### Requirement: Classic transport ID normalization uses effective Lite capability
Untrusted or stale metadata-only Lite markers MUST NOT disable subscription
inline-ID normalization on classic HTTP or compact egress after the marker is
removed. Trusted incremental WebSocket/forwarded continuity and effective Lite
tools MUST preserve their existing identity and ciphertext contracts.

#### Scenario: Metadata-only Lite marker reaches classic transport
- **GIVEN** complete inline message or function call and a Lite marker without recognized Lite tools or trusted continuity
- **WHEN** classic HTTP or compact serializes the request
- **THEN** persisted lookup item IDs are removed while call IDs and content remain unchanged
- **AND** no unsupported Lite marker is forwarded

### Requirement: HTTP bridge publication fences retirement atomically
A subsequent ordinary request MUST recheck retirement under the same pending
lock as its queue publication, with no intervening suspension. A draining owner
MUST retain its identity until fenced settlement and receive no overlapping ordinary send.
Existing cancellation-only retirement MAY close and settle that generation once
no visible demand remains; it MUST NOT leak permits or reservations.
An explicitly authorized detached unanchored handoff MUST retain its exception.

#### Scenario: Owner detaches during request preparation
- **GIVEN** B passed the early retirement check and suspended before admission
- **WHEN** A detaches, marks its generation retiring and frees execution while draining
- **THEN** B is refused before pending publication or upstream send
- **AND** B releases its own permits and reservation without disturbing A

#### Scenario: Optional prewarm meets a retiring generation
- **GIVEN** prewarm suspended during settings or preparation and an accepted owner detached meanwhile
- **WHEN** prewarm reaches pending publication on that retiring generation
- **THEN** no generate:false frame is sent and its own permits are released
- **AND** skipping optional prewarm MUST NOT revoke a captured authorized ordinary handoff

#### Scenario: Failed prewarm already belongs to terminal finalization
- **GIVEN** terminal processing claimed a prewarm request and published its failure before finishing lease cleanup and retirement
- **WHEN** the prewarm consumer handles that failure or its deadline/cancellation
- **THEN** consumer cleanup MUST NOT release execution owned by terminal processing
- **AND** subsequent ordinary input MUST wait until that finalization publishes its retirement fence

### Requirement: Queued HTTP bridge requests retain their original account binding
An HTTP submit MUST capture its expected bridge account before its first await.
After execution waiting and atomically at ordinary or prewarm publication, a
changed account MUST cause an explicit local refusal before another create lease
or upstream send. Same-account reconnect MUST remain permitted. Refusal MUST
release only the waiting request's resources and preserve the active owner's
recovery. Input, file ownership, ciphertext and installation metadata MUST NOT be
silently reprojected onto the replacement account.

#### Scenario: Accepted owner recovers on another account while a request waits
- **GIVEN** A owns execution and B is queued outside pending publication
- **WHEN** safe recovery of A changes the bridge account before B is admitted
- **THEN** B is refused locally with no stale-account lease or upstream create
- **AND** this applies to pinned-file and account-neutral B without interrupting A

#### Scenario: Preparation and optional prewarm observe an account replacement
- **WHEN** the bridge account changes during image preparation or optional warmup settings retrieval
- **THEN** neither ordinary nor warmup input is sent with the captured account's stale identity
- **AND** same-account replacement sockets remain eligible
