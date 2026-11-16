## ADDED Requirements
### Requirement: HTTP operation lifecycle has one owner
The HTTP bridge MUST separate operation, attempt, continuity lane and physical connection identities. Only one controller MUST authorize effects and terminal settlement for each operation. Events from obsolete attempts or connection generations MUST NOT mutate the active operation.
#### Scenario: Late old generation output
- **GIVEN** a pre-publication attempt was replaced
- **WHEN** an event arrives from its old generation
- **THEN** the event cannot publish content or settle the replacement
#### Scenario: A dispatch authorization cannot be reused
- **GIVEN** an initial or replacement attempt has a controller-issued dispatch authorization
- **WHEN** the authorization is stale, already consumed, expired, cancelled or inconsistent with the actual owner policy
- **THEN** the sole physical send boundary refuses the send before response.create crosses the socket
#### Scenario: Interrupted terminal effects resume under one owner
- **GIVEN** normal terminal settlement has completed only some resource effects
- **WHEN** cancellation or an adapter exception requires compensation
- **THEN** compensation resumes the remaining effects under the same operation authority
- **AND** completed reservation effects cannot charge twice
- **AND** retained failed-attempt circuit evidence can still settle independently
### Requirement: Affinity pooling preserves continuity
A replica MUST support up to seven compatible ordinary lanes per affinity/account/API/model/tier group, subject to existing account and physical limits. Specific response and turn anchors MUST select their exact lane; conflicting specific anchors MUST fail closed.
An exact pool-origin continuation MUST retain the existing verified, operation-fenced full-resend recovery contract after an explicit stale-anchor rejection. Moving its turn alias to an account-neutral recovery bridge MUST require an explicit captured source-owner and alias fence; the private pool namespace alone MUST NOT authorize that transfer. A moved alias or changed source API scope, account, owner epoch or local generation MUST prevent transfer and replacement dispatch.
#### Scenario: Independent same-key agents
- **GIVEN** seven complete independent requests with one cache key and available account capacity
- **WHEN** they submit concurrently
- **THEN** none waits for another response solely because their cache keys match
#### Scenario: Broad session headers retain group semantics
- **GIVEN** complete independent requests share a broad session or thread header without a specific response or turn anchor
- **WHEN** seven ordinary lanes are executing and an eighth request arrives
- **THEN** the eighth waits within the bounded compatible group
- **AND** exact anchored work keeps its own lane and queued cancellation preserves other owners
- **AND** eligible complete independent header lanes can rotate between responses while genuine delta-only sessions remain protected
#### Scenario: An active account becomes ineligible for new independent work
- **GIVEN** existing lane work uses account A and the current API assignment now permits only B
- **WHEN** a new complete independent same-group request arrives
- **THEN** eligible B capacity can serve the new operation
- **AND** existing A tickets and exact anchors retain their original ownership
#### Scenario: A pool-origin exact continuation recovers a rejected anchor
- **GIVEN** an exact response and turn anchor identify a pool lane and a complete full-resend is verified under the existing durable operation fence
- **WHEN** the owner explicitly rejects the previous response and the captured source-owner and alias fence still matches
- **THEN** the one permitted account-neutral recovery transfers only that turn alias through reversible registration
- **AND** a moved alias or stale owner proof fails closed before a replacement send
### Requirement: Publication fences transport replay
Readable reasoning, text, tool calls and unknown output MUST commit publication before downstream enqueue. A single ordinary transport replay MAY replace an unpublished lifecycle/opaque-reasoning prefix when complete-input, same-account, tool-safety, ownership and deadline proofs hold. The client MUST observe one lifecycle and a stable response ID with an exact winning-upstream continuation binding.
#### Scenario: Restart after visible reasoning
- **GIVEN** readable reasoning was enqueued downstream
- **WHEN** upstream closes with 1012
- **THEN** the operation terminates without replay
#### Scenario: Telemetry between opaque items
- **GIVEN** an unpublished opaque reasoning prefix and an open publication boundary
- **WHEN** noncommitting telemetry or lifecycle frames arrive between opaque items before a permitted replacement
- **THEN** those frames cannot flush the opaque prefix downstream
- **AND** a successful replacement publishes no abandoned opaque item or ciphertext
#### Scenario: A replacement completes without a created event
- **GIVEN** an unpublished attempt was replaced after its public lifecycle began
- **WHEN** the winning replacement first emits a supported terminal event
- **THEN** every public lifecycle frame retains the original public response ID
- **AND** a subsequent anchored delta after successful completion translates to the winning physical response ID
#### Scenario: Replacement uses a shorter numeric lifecycle
- **GIVEN** the first attempt published numbered lifecycle events before replacement
- **WHEN** a replacement publishes output with an equal or lower upstream sequence number
- **THEN** HTTP publication assigns a strictly greater public sequence number
- **AND** discarded buffered events do not consume public sequence numbers
#### Scenario: Projected continuation loses its physical binding
- **GIVEN** a public response ID was projected from a winning replacement response
- **WHEN** the owner-process binding is missing or its lane, account, generation or owner epoch differs
- **THEN** an anchored delta MUST fail closed without upstream dispatch
- **AND** an existing owner-fenced durable marker distinguishes the projected ID from an ordinary upstream ID
#### Scenario: Cancellation while a replacement connection opens
- **GIVEN** an unpublished attempt has authorized replacement
- **WHEN** downstream cancellation closes the operation while reconnect is suspended
- **THEN** cleanup MUST close operation authority before waiting for the reader lifecycle lock
- **AND** the replacement MUST NOT send another response.create

### Requirement: One original deadline governs local HTTP operation handoffs
HTTP receive timers, expired-request settlement and local proven-unsent transport handoffs MUST use the original immutable operation deadline. Pool waiting and re-preparation MUST NOT create a new total streaming budget. Connect, queue, awaiting-created and idle clocks MUST remain distinct; downstream keepalives MUST NOT extend the upstream total deadline. Direct downstream WebSocket timing MUST retain its existing contract.
#### Scenario: Replacement accepted near the original deadline
- **GIVEN** pool waiting or re-preparation consumed most of the original HTTP budget
- **WHEN** an upstream replacement is accepted shortly before that deadline
- **THEN** the receive watchdog expires at the original deadline
- **AND** later output or downstream keepalives cannot extend it

### Requirement: Every pool lane has an independent physical identity
All seven lane identities, including lane zero, MUST be private to the compatible API/cache/model/tier/replica group. Authoritative physical account ownership MUST be retained separately from the logical queue fence through an explicitly authorized account change. Such a changed transport MUST NOT be advertised as warm for the original account.
#### Scenario: Distinct model or tier leaders
- **GIVEN** complete independent requests with one API/cache identity and distinct model or tier groups
- **WHEN** each group elects its first lane
- **THEN** those first lanes have distinct physical keys
#### Scenario: A pinned continuation occupies its logical lane
- **GIVEN** a known exact local continuation is executing on a pool lane
- **WHEN** a complete independent same-group request arrives and a sibling lane is free
- **THEN** the continuation retains its exact lane
- **AND** the independent request can dispatch on the sibling before that continuation completes
#### Scenario: Conclusive capacity recovery changes the physical owner
- **GIVEN** a complete independent request has an output-free capacity rejection and a current controller grant permitting a different account under the existing ownership proofs
- **WHEN** its replacement dispatches on account B
- **THEN** the logical lane order remains intact and the physical connection is not advertised as warm for original account A
- **AND** the private routing key cannot override the grant with an unrelated hard-owner constraint
- **AND** transport recovery and exact, file, age and durable-operation owner restrictions retain their existing fences
### Requirement: Pool work participates in bounded scheduling and restart readiness
Pool admission MUST preserve the existing per-lane queue limit and ready FIFO ordering. Unsent independent work MAY move to any free compatible lane; exact anchors MUST remain ordered on their remembered logical lane. Cancellation MUST remove only the caller's ticket and cannot release a successor. Bootstrap waiters MUST survive another caller's cancellation. Logical pool work MUST block restart even before physical connection creation. Idle account hints and physical-key references MUST be bounded while retaining active/queued ownership evidence.
#### Scenario: Bootstrap before a socket
- **GIVEN** a pending pool bootstrap without a physical WebSocket
- **WHEN** activity readiness is sampled
- **THEN** http_bridge_restart_blocking is true
- **AND** the logical queued-work count is nonzero
#### Scenario: Pinned Bifrost conversion
- **GIVEN** Bifrost 2.5.1 forwards Anthropic Messages to this HTTP bridge with synthetic upstream I/O
- **WHEN** an opaque-only prefix is replaced after 1012
- **THEN** the client receives one message_start and one successful message_stop
- **AND** no abandoned opaque content is published
- **AND** readable-output failure and successful tool-use retain their Anthropic contracts

## MODIFIED Requirements

### Requirement: Idle unanchored prompt-cache physical WebSocket lifetime
For an original self-contained full-resend HTTP request in the canonical soft prompt-cache registry lookup or a proven independent affinity-pool lane, the proxy MUST rotate a physical upstream socket aged at least 3000 monotonic seconds before submitting that request only when the original and prepared request have no specific previous-response/conversation/turn continuity and its complete original message/function-call/function-output/reasoning items contain no reference-only or unknown opaque shapes. A broad session or thread header used only for pool group/locality MUST NOT classify that independent physical lane as a Codex continuity session. Genuine Codex continuity sessions, exact anchors and delta-only input MUST retain their existing protection. Every function-call ID MUST have exactly one subsequent full function output; duplicate calls, duplicate outputs, orphan outputs and pending calls MUST make the request ineligible. The proof MUST NOT depend on a durable row or previous input fingerprint: unanchored Responses input defines the new context, while a soft prompt-cache key is only a cache hint.
The proxy MUST retain original input content, order, tool call IDs, ciphertext and prompt_cache_key, require the old account for replacement, and preserve the original reservation and deadline with one upstream dispatch. The proxy MUST NOT lose that owner restriction through outer HTTP fallback or any subsequent reconnect or reroute. A later recovery that cannot preserve the owner MUST fail closed before another send.
The age branch MUST NOT detach a generation with any literal pending/draining request, queued or admission owner, another operation's pool pin, reservation, handoff, resource close owner, or reader currently processing/persisting a frame. Only the current unsent complete independent pool head's own resource pin MAY be discounted; other queued exact-anchor owners MUST retain their generation. Uncertain lock acquisition MUST skip rotation. Physical opening time MUST reset on a successful replacement and MUST NOT reset on reuse or durable refresh.
Forwarded requests, Responses-Lite requests, recovery-key paths and opaque encrypted function arguments MUST remain ineligible. The age policy MUST NOT construct or send a projected history.
This requirement does not add transparent retry after accepted model output and does not cover a busy bridge or raw downstream WebSocket.

#### Scenario: Hour-old idle bridge receives a fresh complete history
- GIVEN a soft prompt-cache socket opened 3593.4 seconds ago, idle TTL 3600 and no owners
- WHEN a complete original unanchored HTTP request arrives
- THEN the old socket receives no new create, a same-account replacement receives exactly one create with unchanged context/PCK, and the response completes.
#### Scenario: Old reader persists a popped terminal
- GIVEN an aged socket with an empty pending deque while its reader persists a terminal
- WHEN another eligible lookup runs
- THEN age rotation does not close or cancel that reader.

#### Scenario: Complete cancelled function output
- GIVEN a full original history contains a function_call_output with a nonempty call_id, full string or complete native text-array output, and native status incomplete
- WHEN the other age and ownership guards pass
- THEN the proxy MUST accept that complete output for rotation and preserve its status and content verbatim.
- AND incomplete function calls, reference-only reasoning and unknown opaque item shapes MUST remain ineligible.

#### Scenario: Required replacement owner cannot open a socket
- GIVEN the age policy detached an eligible socket and retained its account as required
- WHEN that account is unavailable, selection returns another account, or its replacement handshake fails
- THEN the proxy MUST fail without sending the request on another account or replaying it through an unpinned raw HTTP fallback.

#### Scenario: No durable or historical prefix record
- GIVEN a canonical soft-PCK non-Codex socket and a complete unanchored original input with uniquely paired function calls and outputs
- WHEN no durable row or prior input fingerprint is available and all age/ownership guards pass
- THEN the proxy MUST permit same-account rotation without rewriting the input.

#### Scenario: Predecessor close holds the only registry slot
- GIVEN an eligible aged socket occupies the only available registry slot and its close is delayed
- WHEN the age policy plans its replacement
- THEN the proxy MUST account for that predecessor as a planned synchronous close, await bounded cleanup outside registry locks, and admit the replacement only after capacity is available.
- AND the age policy MUST NOT evict unrelated idle sessions merely because its own predecessor close is pending.

#### Scenario: Capacity refusal after opening the replacement
- GIVEN same-account age replacement succeeded but its first submission is refused by account response-create capacity
- WHEN a different account is selectable
- THEN the proxy MUST retain the required owner or fail closed without dispatching the original input to that other account.

#### Scenario: Owner lookup fails after age detachment
- GIVEN the age policy detached an unowned predecessor and has not reached its planned-close loop
- WHEN an owner/ring lookup fails or the request is cancelled
- THEN an independently owned cleanup task MUST close the predecessor exactly once and release its detached capacity entry without closing unrelated sessions.
