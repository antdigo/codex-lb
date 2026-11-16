## 1. Native and containment
- [x] 1.1 Add explicit validated native executable option to early bootstrap; prove rejection outside allowed roots.
- [x] 1.2 Separate wire head/body phases and prove truncation/cancellation/peer isolation.
## 2. Replay and fixture privacy
- [x] 2.1 Extend discovery sanitizer; preserve pairing and deterministic anonymization.
- [x] 2.2 Reconcile rejection/schema tests against canonical replay contracts.
## 3. Backend timing and ownership
- [x] 3.1 Pin limiter clock, source-dispatch clock, SSE publication phases and PG read-completion barriers.
- [x] 3.2 Repair real session/context-manager harnesses and expose owned-task leaks at the originating test.
## 4. Frontend
- [x] 4.1 Prove local Suspense boundary then implement it.
- [x] 4.2 Own QueryClient cleanup, scope UI queries/paste values, split JSDOM CRUD and retain full browser CRUD.
## 5. Verification and CI
- [x] 5.1 Manual full CI, PG regression selection, Helm cases and metrics extra.
- [ ] 5.2 Full backend/frontend/Rust/native/PG verification; independent final review; candidate fingerprint and evidence.

## 6. Final review correction pass
- [x] 6.1 Prove failed create-owner receipt resumption through HTTP detach, preserving independent settlement.
- [x] 6.2 Verify public string normalization and exact-repeat locality; avoid new state for unreachable unvalidated shapes.
- [x] 6.3 Preserve invalid discovery classifications and valid defer_loading flags through rebuild.
- [ ] 6.4 Run affected regressions and complete final verification on the corrected source.
- [x] 6.5 Resolve final full-run cancellation failures with owned heartbeat transactions and startup iterator handoff; verify native HEAD startup separation.
- [x] 6.6 Bind sequential WebSocket response batches to actual sends and verify immediate and reader-idle follow-ups.
- [x] 6.7 Prove shared terminal cleanup deadlines and owned-child drain with causal virtual time.
- [x] 6.8 Prove 100-way account persistence concurrency with an owned causal barrier and a serialization-negative control.
- [x] 6.9 Drive the SSE heartbeat timing harness through the existing virtual scheduler seam; prove ordering and host-stall resistance.
