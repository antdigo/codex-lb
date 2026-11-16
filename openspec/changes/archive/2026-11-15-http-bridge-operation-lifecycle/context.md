# Operation lifecycle and rollout context

A prompt cache key is a reuse hint shared by independent agents. It does not
identify a conversation. Independent complete requests use a bounded group of
seven logical lanes; an exact response or turn anchor keeps its lane and order.
Broad session/thread headers retain group locality rather than selecting a
single physical lane. Account eligibility for new independent work is checked
against the current API assignment without moving existing anchored work.
The account stream cap remains eight, with one slot reserved for the existing
recovery admission path. Physical sessions remain subject to the existing global
session limit and idle eviction policy.

For example, seven agents can submit complete conversations with the same cache
key concurrently. A follow-up containing a previous response ID waits for that
response's lane, while an independent eighth request can use whichever compatible
ordinary lane next becomes available. A lane queue survives replacement of its
physical WebSocket. Waiting references pin the required generation against idle
eviction; these pins do not acquire account stream leases.

The publication boundary separates replaceable work from output already exposed
to the client. Readable reasoning is output. Only an understood opaque reasoning
prefix can remain unpublished, bounded to 256 KiB. An allowed replacement keeps
the public response ID and translates an anchored continuation to the winning
physical response ID. Item IDs and ciphertext are retained unchanged. A lost
process-local binding fails closed, using an existing owner-fenced durable alias
marker to distinguish projected IDs. No process-restart recovery or database
migration is introduced.

Dispatch authorizations are single-use and checked at the physical send boundary.
Close, timeout, cancellation and terminal observations produce controller-owned
decisions; existing socket, lock, lease and persistence code executes those
decisions. Resumable resource receipts preserve compensation after an interrupted
terminal write. The original absolute deadline spans pool waits, re-preparation
and a proven-unsent local transport handoff.

An abandoned physical attempt may have consumed tokens without returning usage.
Its usage is marked unknown; reported final usage belongs to the winning attempt.
The system cannot infer an unreported upstream charge.

Specific continuations retain the existing verified full-resend recovery path
even when their physical owner originated in the independent pool. For example,
an exact response/turn follow-up whose owner explicitly rejects the old response
may replay its verified complete context under the existing durable operation
fence. Moving that particular turn alias requires a captured source-owner/alias
proof and reversible registration; a private pool key is not itself permission
to move aliases. Changed ownership or a concurrently moved alias fails closed.

## Validation and deployment boundary

The source change uses synthetic upstream sockets, temporary databases and an
isolated Bifrost 2.5.1 process for Anthropic conversion acceptance. Production
credentials and prompts are not acceptance fixtures. The normal test suite skips
this optional binary test unless `CODEX_LB_BIFROST_ACCEPTANCE_BINARY` is supplied.

Deployment is separate from source implementation. Before activation, seal the
candidate with its base revision, complete source hashes and patch digest; retain
the previous runtime selection. Stop and back up application data using the
existing service-user SQLite backup procedure. Since this change does not alter
the database schema, rollback selects the previous runtime while retaining live
database and refreshed authentication tokens. Do not restore an old database
snapshot merely to roll back this code change.

Process-local projected continuation bindings do not survive a runtime switch.
Drain active operations and resume clients with complete unanchored context,
without the old `previous_response_id` or turn-state anchor. This is especially
necessary when selecting an older runtime that has no projection translator.
Retaining the live database preserves tokens and routing records, not the lost
process-local translation map.

The canary gate requires at least 65 minutes and 100 comparable requests. Observe
correctness, active/queued operations, session and lease cleanup, failure kinds
and comparable cache-hit ratios. Stop expansion on duplicate or misrouted output,
leaked ownership/leases, or a cache-hit ratio drop greater than five percentage
points. Synthetic acceptance is evidence of conversion behavior, not evidence
that this operational canary has run.
