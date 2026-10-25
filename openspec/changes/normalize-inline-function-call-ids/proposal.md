## Why

Claude Code history converted by Bifrost can contain a complete inline function_call with a synthesized item id. The subscription backend can interpret that ID as a persisted-item lookup and reject the request. A two-turn max-reasoning reproduction failed with the function-call ID and succeeded when only that ID was omitted, retaining call_id and the original encrypted reasoning.

## What Changes

- Omit redundant item id only from complete plaintext inline function calls on classic HTTP Responses/compact subscription egress.
- Preserve request models, owner-forwarding payloads, replay-safety evidence, call_id, encrypted reasoning/calls, explicit references, and Responses Lite.
- Preserve direct client WebSocket IDs on initial dispatch and fresh replay.
- Carry original function-call-ID ownership evidence through HTTP recovery and request-state reconstruction.

## Capabilities

### Modified Capabilities
- `responses-api-compat`: normalize subscription wire metadata without weakening replay ownership.

## Impact

Shared request serialization, HTTP bridge and WebSocket serialization/ownership call sites, regression tests, and OpenSpec documentation. One boolean is added to existing per-request state. No configuration, dependencies, database migration, session cache, or new retry/account-switch policy.
