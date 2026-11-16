## ADDED Requirements

### Requirement: HTTP bridge execution waits are isolated from active responses

An HTTP bridge MUST execute at most one response at a time on its upstream socket. A subsequent create MUST wait locally within the existing queue and admission/request budgets. Its wait timeout or cancellation MUST affect only that request, without upstream send/replay, reader cancellation, sibling failure, continuity changes or account penalty. Separate sockets MUST retain independent concurrency.

#### Scenario: a waiting request times out while its sibling streams
- **GIVEN** A is accepted and streaming tool arguments on an HTTP bridge
- **WHEN** B on the same bridge exhausts its local admission wait
- **THEN** B receives an explicit local wait error without an upstream create
- **AND** A continues to response.completed on the same socket and account
- **AND** the next request can reuse that socket with its cache key unchanged

#### Scenario: a waiting request is cancelled
- **GIVEN** A owns execution and B waits locally
- **WHEN** B is cancelled
- **THEN** A retains its execution permit, stream and upstream reader
- **AND** B releases its queue registration and API-key reservation exactly once

### Requirement: HTTP bridge execution ownership survives recovery and cleanup

Execution ownership MUST remain with its request through safe same-session recovery.
Terminal processing, pre-dispatch failure, cancellation, prewarm timeout and reader
failure MUST release only their own ownership. Admission MUST be fenced before a
failed owner wakes a queued successor. Late events MUST NOT target unsent waiters.

#### Scenario: the executing request terminates or recovers
- **WHEN** an owner completes, fails, or performs eligible pre-response recovery
- **THEN** completion and failure release ownership through their existing cleanup
- **AND** recovery retains exclusive ownership until that attempt terminates
- **AND** a queued successor never dispatches onto a retired generation

#### Scenario: prewarm and ordinary requests share execution admission
- **WHEN** a prewarm executes or times out before an ordinary request
- **THEN** both paths respect one execution owner and release ownership on cleanup
- **AND** a successful ordinary request is not blocked by a leaked prewarm permit

#### Scenario: downstream cleanup meets terminal bookkeeping ownership
- **GIVEN** a terminal continuation has removed A from pending ownership
- **WHEN** downstream cleanup detaches A while terminal bookkeeping is still running
- **THEN** B MUST remain blocked until the terminal continuation releases execution
- **AND** a bookkeeping abort MUST fence the generation before B is awakened

#### Scenario: global admission cannot restart the wait budget
- **GIVEN** an HTTP admission deadline is ten seconds and execution waits six seconds
- **WHEN** the global create controller remains occupied beyond the remaining four seconds
- **THEN** admission MUST fail at the original deadline without dispatching the waiter
- **AND** existing create/account permits MUST be released through bounded cleanup

### Requirement: HTTP execution admission is independent from create admission

Execution admission MUST NOT change account-create or response-created admission:
those permits MUST retain their acceptance-time release semantics. Execution state
MUST be bounded by the existing bridge and queue limits. Direct downstream WebSocket
behavior MUST remain unchanged.

#### Scenario: response acceptance releases create admission only
- **WHEN** an HTTP bridge receives response.created for its executing request
- **THEN** account-create and response-created admission are released
- **AND** execution remains owned until terminal processing or fenced cleanup
