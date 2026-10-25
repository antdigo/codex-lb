## Why

An automatic pre-created retry can replace a shared HTTP bridge socket while another accepted response still belongs to the old socket. The stranded response remains pending and makes anonymous output events on the replacement socket ambiguous. The bridge then drops the new response's intermediate events, leaving clients with creation and completion but no output-item lifecycle.

## What Changes

- Refuse automatic socket replacement for any pre-created retry when another request remains pending, including accepted and detached/draining requests.
- Preserve the reader failure funnel's terminal settlement of every affected request; do not guess anonymous event ownership or replay an accepted sibling.
- Cover the shared-socket bridge path and retain single-request replay behavior.

## Impact

- Affects HTTP bridge transport-close recovery only; no settings, database, or native protocol changes.
- Replaces an unsafe replay with the existing retryable stream failure when several turns share a failed socket.
- Public stream normalization's accepted-envelope/missing-item behavior is investigated separately from this socket ownership fix.
