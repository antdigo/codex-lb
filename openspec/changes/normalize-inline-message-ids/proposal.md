## Why

The function-call-ID fix does not cover synthesized message IDs in Claude Code history converted by Bifrost. In an affected reused subscription session, an unchanged request failed with persisted-item lookup; removing only six plaintext assistant message IDs completed successfully; restoring the IDs failed again. The same body was accepted in a fresh diagnostic namespace, so a fresh-session-only check missed the defect.

## What Changes

- Extend classic HTTP Responses/compact wire normalization to completed inline plaintext assistant messages.
- Preserve caller models, forwarding, replay-safety evidence, encrypted reasoning, references, direct client WebSocket, and Lite contracts.
- Generalize the existing original-inline-ID ownership flag so removing message IDs cannot permit account switching or lose ownership through trim/reprepare.

## Capabilities

### Modified Capabilities
- `responses-api-compat`: normalize redundant inline message IDs without weakening original ownership.

## Impact

The existing wire normalizer, its transport call sites and existing request-state flag, regression tests, and OpenSpec. No configuration, dependencies, migration, new retry, or additional retained state.
