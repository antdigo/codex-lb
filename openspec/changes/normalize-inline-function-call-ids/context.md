## Reproduction and rationale

A two-turn tool-result replay can fail with `Supplied input item IDs require
persisted-item lookup that is not supported by rustponsesapi.` Removing only the
complete plaintext function-call item id, while preserving call_id and encrypted
reasoning, resolves this shape. This does not imply every ID causes an error or
that IDs alone invalidate prompt caching.

Classic subscription HTTP egress omits the redundant top-level id. The caller,
forwarding and replay-safety views retain their IDs, as do native WebSocket and
Responses Lite requests. Original ownership evidence must be recorded before
wire normalization and survive request reconstruction, anchor trim and replay.

## Validation

Synthetic regressions exercise request conversion, canonical/trailing-slash HTTP
routes, bridge reconstruction and direct WS/Lite serialization. Current-main
results and cost measurements belong in the PR validation section. No private
request history or host-specific deployment files are included.

## State cost

One boolean is retained on existing request state. No per-session content cache,
new retry, account-switch policy, setting, dependency or migration is introduced.

## Current-main measurements

On Python 3.13, validate + to_payload + compact JSON serialization measured
102.62 → 122.96 microseconds/op for a one-message request and
13,473.97 → 14,136.49 microseconds/op for 400 replay items plus 170 tools.
Peak traced allocation was 3,954 → 3,954 bytes and 727,454 → 715,735 bytes
respectively. These same-machine medians are local serialization measurements
under concurrent test load, not network latency or an SLA. No content is
retained beyond existing request state.
