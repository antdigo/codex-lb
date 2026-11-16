# Full-suite reliability

## Why
The 2026-11-16 full run found failing replay/sanitizer tests, nondeterministic native HTTP and PostgreSQL phase scheduling, slow UI journeys and cleanup warnings. Individual retries do not establish a green candidate.

## What Changes
- Prove response-head/body phases independently and keep existing transport retry contracts.
- Extend privacy-safe fixture rebuilding for portable client discovery.
- Replace wall-clock races with explicit clocks and completed-read barriers.
- Bound lazy dialog rendering locally and own test queries/tasks through teardown.
- Provide an explicit contained native-test binary option and full manual CI including coverage/Helm/PG.

- Keep create-owner resource receipts incomplete until retained account leases are actually released.
- Verify public string-input normalization preserves existing exact-repeat locality without new cache state.
- Preserve invalid discovery semantics and supported deferred-loading flags in offline fixtures.

## Impact
No production database access, schema migration, service activation or expansion of account/continuation authority. Full CI on the exact candidate is the acceptance gate.
