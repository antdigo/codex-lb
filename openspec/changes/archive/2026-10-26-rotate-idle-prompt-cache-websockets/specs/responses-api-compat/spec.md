## ADDED Requirements
### Requirement: Idle unanchored prompt-cache physical WebSocket lifetime
For an original self-contained full-resend HTTP request in the canonical soft prompt-cache registry lookup, the proxy MUST rotate a physical upstream socket aged at least 3000 monotonic seconds before submitting that request only when the original and prepared request have no previous-response/conversation/turn/session continuity, the session is not a Codex continuity session, and its complete original message/function-call/function-output/encrypted-reasoning items contain no reference-only or unknown opaque shapes. Every function-call ID MUST have exactly one subsequent full function output; duplicate calls, duplicate outputs, orphan outputs and pending calls MUST make the request ineligible. The proof MUST NOT depend on a durable row or previous input fingerprint: unanchored Responses input defines the new context, while a soft prompt-cache key is only a cache hint.
The proxy MUST retain original input content, order, tool call IDs, ciphertext and prompt_cache_key, require the old account for replacement, and preserve the original reservation and deadline with one upstream dispatch. The proxy MUST NOT lose that owner restriction through outer HTTP fallback or any subsequent reconnect or reroute. A later recovery that cannot preserve the owner MUST fail closed before another send.
The age branch MUST NOT detach a generation with any literal pending/draining request, queued or admission owner, reservation, handoff, resource close owner, or reader currently processing/persisting a frame. Uncertain lock acquisition MUST skip rotation. Physical opening time MUST reset on a successful replacement and MUST NOT reset on reuse or durable refresh.
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
- GIVEN a full original history contains a function_call_output with a nonempty call_id, full string output, and native status incomplete
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
