## Decision

Normalize at the subscription wire boundary. A complete plaintext function call has a nonempty string call_id, nonempty string name, string arguments, and neither nonempty encrypted_content nor nonempty encrypted_function_args. Omit its redundant item id from the serialized copy, keeping every other field.

Responses Lite is excluded, including metadata-only incremental continuations. Move its existing metadata predicate to the lower request-schema layer and re-export it from the client module to avoid a dependency cycle. Preserve request objects, owner-forwarding payloads, and IDs used for replay-safety classification.

Direct client WebSockets explicitly disable stripping for both initial frames and fresh replay. HTTP requests may still use a WebSocket upstream; that transport choice does not turn them into direct client WebSocket requests.

## Ownership invariants

HTTP recovery also classifies serialized frames. Record original function-call-item-ID evidence in one boolean on existing request state before stripping. Keep state-aware ownership checks pinned to the original account even when the normalized frame looks self-contained. A preparation closure carries the evidence through temporary/fresh/trimmed state reconstruction.

Only an existing, independently proven account-neutral projection can reset that evidence. Ordinary trimming cannot. Verified same-account fresh replay remains available. This preserves the existing policy rather than introducing another account-switch or retry path.

## Alternatives

Changing Bifrost-generated IDs did not eliminate the minimal max-reasoning reproduction. Deleting reasoning IDs risks an encrypted-content/item-ID mismatch. Reconnecting after every request sacrifices session reuse. A bounded transformation of complete plaintext function calls addresses the observed metadata problem without those changes.

## Cost and verification

The normalizer adds one bounded input traversal over the existing wire copy. It retains no content; the ownership flag lives for the existing request-state lifetime. Cover wire serialization, HTTP routes, direct WebSocket frames/fresh replay, Lite continuations, and ownership after state reconstruction. Verify a live max-reasoning/tool-result roundtrip. See [context.md](context.md) for observations and serialization measurements.
