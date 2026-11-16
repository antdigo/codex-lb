# HTTP bridge execution and create admission

The old ten-second deadline covered waiting for a complete predecessor response
and acquiring create admission. Observed successful predecessors lasted 13–35
seconds, so otherwise healthy concurrent requests failed. A timed-out request
had not been sent upstream; its sibling was correctly preserved.

Use the existing pre-response eventless policy for ordinary execution waiting:
the minimum of the stuck-startup threshold (normally 300 seconds), stream idle
timeout and bridge request budget. Clamp it further to the retained absolute
request deadline, including retry states whose started_at was reset. Afterwards
start the ordinary ten-second create-admission deadline; account/session/global
admission may consume it but cannot reset it or exceed the request deadline.
Optional prewarm keeps the existing short wait rather than extending warmup.

Alternatives: increasing the global admission constant would also loosen account
and connection admission. Waiting for the entire two-hour bridge budget would
hold a silent request excessively. Neither is needed. No upstream multiplexing,
parallel accepted replay or socket rotation is introduced.

Example: request B can wait 25 seconds for A, then acquire create admission.
If its original deadline is 28 seconds it has at most three seconds left; if the
deadline is 100 seconds it gets the usual ten-second create budget. Cancellation
of B never releases A's ownership. The queue remains bounded by its current limit.

Create admission also requires an absolute boundary around preparatory work: settings refresh
and account lease acquisition were awaited outside its deadline, and session
admission reused a stale relative timeout. A scheduler fail-after scope now bounds
preparatory awaits and session acquisition; remaining time is recalculated just
before acquiring that gate. Cleanup runs after the scope exits. Direct downstream
WebSockets keep their existing unbounded preparatory phase and short gate budget.

No new payload buffer, cache, queue limit, database state or per-chunk work is
introduced. The budget transition itself is O(1) time and temporary memory.
Existing semaphore cancellation may scan O(q) waiters. An existing bounded queue
now retains requests for longer: payloads, HTTP tasks and reservations may live
for up to the pre-response policy bound rather than ten seconds. Their memory is
O(sum of retained request sizes), with existing bridge/queue bounds unchanged.

Queued account identity is deliberately conservative: capture the bridge account
before any submit preparation and refuse an unsent request locally if an older
owner's permitted recovery changes it. This avoids projecting pinned files,
opaque state or installation metadata onto another account and avoids charging
its create lease to a stale owner. A portable waiter may need one client retry
after a cross-account recovery; same-account socket recovery remains allowed.

Eligible prewarm shares the same original deadline over settings retrieval,
prewarm/lifecycle locks, execution/create admission, send and response waits.
Its warmup state inherits that deadline; fallback derives once from the ordinary
request's original start. The disabled/non-Codex branch returns before allocating
any deadline scope. Optional warmup execution-capacity timeout skips warmup, while
parent deadline exhaustion remains an explicit local refusal.
