## Reproduction and interpretation

A two-turn max-reasoning/tool-result request failed with `Supplied input item IDs require persisted-item lookup that is not supported by rustponsesapi.` Keeping the request on the same session and omitting only the complete plaintext function-call item id made it complete. Omitting only the reasoning ID did not resolve it.

This supports the bounded function-call transformation; it does not establish that all IDs break prompt caching or that every persisted-item error has this cause. Keep call_id for call/output pairing and preserve encrypted reasoning together with its original ID.

## Validation evidence

The combined patch was tested against codex-lb v1.24.0 and official Bifrost v2.3.2. A production-route SSE check completed the tool-call and tool-result turns with 130 and 72 reasoning tokens respectively, ending in OK. A separate appended-turn cache probe read 72,448 of 72,683 input tokens from cache.

Regressions cover complete/incomplete/encrypted/reference shapes, caller mutation, direct WebSocket serialization and fresh replay, Lite metadata-only continuations, and original ownership across HTTP state reconstruction. The PR test plan records current local test results and known baseline failures.

## Serialization cost

An earlier same-machine measurement of the combined wire serializer reported small input at 49.12 -> 56.07 microseconds/op and a 772-item history at 14,759.84 -> 14,948.43 microseconds/op. Large-history peak traced memory was unchanged at 739,369 bytes. These are Python wire-serialization measurements, not gateway end-to-end latency measurements.

The implementation retains only one additional boolean on existing request state. It adds no content cache or per-session collection.
