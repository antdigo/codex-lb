## Implementation

- [x] Add failing wire serialization and public-route regression tests.
- [x] Omit redundant plaintext inline function-call IDs at classic subscription egress.
- [x] Preserve replay-safety evidence, encrypted items, explicit references, and Lite continuations.
- [x] Preserve direct client WebSocket IDs on initial frames and fresh replay.
- [x] Preserve original HTTP ownership evidence through reconnect, auth recovery, and trim/reprepare.
- [x] Run targeted regression suites, lint, strict OpenSpec validation, and independent review.
- [x] Verify a max-reasoning/tool-result roundtrip and appended-turn cache reuse through official Bifrost.

## Archive status

The delta validates strictly. Archival was attempted after deployment verification, but the CLI rejected ten pre-existing requirements without SHALL/MUST in the canonical responses-api-compat spec and wrote no changes. Keep this completed change active until that baseline validation issue is resolved; do not bypass validation or rewrite unrelated requirements here.
