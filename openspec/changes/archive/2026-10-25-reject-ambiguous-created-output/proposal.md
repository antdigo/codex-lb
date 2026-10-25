## Why

The shared anonymous-output matcher can fall through after finding several accepted response owners. Visibility and draining fallback rules can then attribute an ambiguous output event to one visible, draining, or still pre-created sibling. A shared prompt-cache socket does not prove that request owns the content.

## What Changes

- Refuse anonymous nonterminal `response.*` output attribution whenever more than one pending request already has a response id, regardless of visibility or draining state.
- Preserve the single-created-owner rule, explicit response-id routing, error/terminal matching, and vendor telemetry behavior.
- Cover the bridge consumer path and audit shared-socket reconnect call sites.

## Impact

- Applies to the matcher shared by HTTP bridges and direct WebSocket bookkeeping.
- No ownership inference, new state, database, settings, or native protocol changes.
