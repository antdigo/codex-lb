## Implementation

- [x] Add failing Responses/compact regression tests for late instructions, append-only replay, missing initial instructions, and attachment boundaries.
- [x] Add regression coverage at streamed/collected HTTP compatibility routes, including trailing slashes.
- [x] Bound instruction hoisting to the initial prefix; preserve later messages and caller input.
- [x] Run normalization, route, bridge, and replay-safety regression suites.
- [x] Validate the OpenSpec change strictly and review the implementation independently.
- [x] Verify cache reuse after appending a turn through Bifrost and the subscription adapter.

## Archive status

The delta validates strictly. Archival was attempted after deployment verification, but the CLI rejected ten pre-existing requirements without SHALL/MUST in the canonical responses-api-compat spec and wrote no changes. Keep this completed change active until that baseline validation issue is resolved; do not bypass validation or rewrite unrelated requirements here.
