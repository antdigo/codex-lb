## MODIFIED Requirements

### Requirement: HTTP bridge execution waits are isolated from active responses

An HTTP bridge MUST execute at most one response at a time on its upstream socket.
An ordinary subsequent create MUST wait locally within the existing queue limits,
the pre-response silence budget and the original request deadline. Its execution
wait MUST NOT consume the short session/global create-admission budget. Wait timeout
or cancellation MUST affect only the waiting request without upstream send/replay,
reader cancellation, sibling failure, continuity changes or account penalty.
Separate sockets MUST retain independent concurrency.

#### Scenario: a healthy predecessor outlives create admission
- **GIVEN** an ordinary waiter has an original deadline of 100 seconds and create admission is ten seconds
- **WHEN** the active sibling finishes after 25 seconds
- **THEN** the waiter MUST still acquire execution and may dispatch once on that same socket
- **AND** the active sibling MUST complete normally without a replacement socket

#### Scenario: create admission starts after execution ownership
- **GIVEN** execution waiting consumes six seconds and the original deadline is 100 seconds
- **WHEN** session/global create admission remains unavailable
- **THEN** its shared ten-second deadline MUST start when execution is acquired
- **AND** no create stage may restart that deadline

#### Scenario: retained request deadline still bounds all waits
- **GIVEN** a retry state retains an earlier absolute request deadline
- **WHEN** execution or create admission exhausts the remaining time
- **THEN** it MUST fail locally without extending that deadline or disturbing the executing sibling

#### Scenario: preparatory account waits cannot extend create admission
- **GIVEN** the original request deadline is ten seconds, retry started_at was reset and execution ownership is acquired at six seconds
- **WHEN** settings refresh or account lease acquisition remains blocked
- **THEN** create admission MUST stop at the retained deadline and cancel its preparatory wait
- **AND** cleanup MUST release only the request's acquired resources

#### Scenario: preparatory work consumes the same create budget
- **GIVEN** execution is acquired at six seconds and settings refresh completes at nine seconds
- **WHEN** the retained request deadline is ten seconds and session or global admission is still occupied
- **THEN** that admission MUST receive only the remaining one second
- **AND** timeout or external cancellation MUST release the account lease exactly once without releasing another request's gate

#### Scenario: optional prewarm keeps short admission
- **WHEN** optional prewarm waits for an occupied execution owner
- **THEN** it MUST retain the short admission wait and existing cleanup semantics

#### Scenario: Optional prewarm execution timeout skips warmup
- **GIVEN** a healthy accepted owner remains active after optional prewarm exhausts its short execution wait
- **WHEN** the ordinary request still has its original request budget
- **THEN** prewarm MUST be skipped and the ordinary request MUST enter its own bounded execution wait
- **AND** the active owner keeps its permit and no warmup create is sent

#### Scenario: waiter cancellation and retirement preserve isolation
- **WHEN** a waiting request is cancelled or its owner generation is retired
- **THEN** it MUST retain the existing bounded cleanup, settlement and retirement fences

#### Scenario: a waiting request times out while its sibling streams
- **GIVEN** A is accepted and streaming tool arguments on an HTTP bridge
- **WHEN** B on the same bridge exhausts its bounded execution wait
- **THEN** B receives an explicit local wait error without an upstream create
- **AND** A continues to response.completed on the same socket and account
- **AND** the next request can reuse that socket with its cache key unchanged

#### Scenario: a waiting request is cancelled
- **GIVEN** A owns execution and B waits locally
- **WHEN** B is cancelled
- **THEN** A retains its execution permit, stream and upstream reader
- **AND** B releases its queue registration and API-key reservation exactly once

### Requirement: HTTP bridge execution ownership survives recovery and cleanup

Execution ownership MUST remain with its request through safe same-session
recovery. Terminal processing, pre-dispatch failure, cancellation, prewarm timeout
and reader failure MUST release only their own ownership. Admission MUST be fenced
before a failed owner wakes a queued successor. Late events MUST NOT target unsent
waiters. The original request deadline MUST remain the outer bound across execution
waiting, session/global create admission and recovery.

#### Scenario: global admission cannot extend the retained request deadline
- **GIVEN** the original deadline is ten seconds and execution waits six seconds
- **WHEN** the global controller stays occupied for the remaining four seconds
- **THEN** admission MUST fail at the original request deadline with existing permits released

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
- **GIVEN** an original HTTP request deadline is ten seconds and execution waits six seconds
- **WHEN** the global create controller remains occupied beyond the remaining four seconds
- **THEN** admission MUST fail at the original deadline without dispatching the waiter
- **AND** existing create/account permits MUST be released through bounded cleanup
