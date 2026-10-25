## Context

See proposal.md for the incident mechanism. A session's pending deque outlives a socket replacement, so a replacement is safe only when the caller owns every pending physical response.

## Decisions

- Keep the early pre-created retry guard to refuse before replay identity, counters, admission or gate state is changed.
- Add the same ownership fence at reconnect entry, before selection or connection mutation, covering fresh-upstream, auth, security and prewarm call sites. Fresh-upstream callers already own `lifecycle_lock`; response retry callers retain the create gate. Prewarm timeout additionally acquires `lifecycle_lock` across its reconnect so a sender that passed its closed check cannot publish after the ownership snapshot.
- Let prewarm explicitly identify only its own disposable warmup state. All other pending states remain protected. The existing prewarm cleanup owns removal and admission settlement of that state.
- Decline shared-socket recovery and retain existing terminal settlement. Replaying every sibling would duplicate accepted work; FIFO or most-recent-response attribution would not prove content ownership.

## Risks and rollout

Shared-socket failures become explicit retryable stream failures rather than partly recovered streams. Sole-request recovery stays available. The check runs only during recovery and retains no new memory. Ship as a Python package update using normal drain/restart procedures; rollback restores the previous package. No migration or native helper rebuild is involved. This change has not been deployed by the investigation.
