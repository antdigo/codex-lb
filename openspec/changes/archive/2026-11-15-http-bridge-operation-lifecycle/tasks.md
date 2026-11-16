## 1. Specification and regressions
- [x] Record accepted architecture and admission contract changes.
- [x] Add route-level regressions and deterministic failure schedules.
## 2. Operation ownership
- [x] Introduce typed controller/attempt events, single settlement and stale-event fencing.
- [x] Integrate HTTP submit, reader, timers and cancellation with parity tests.
## 3. Publication and recovery
- [x] Integrate bounded publication gate and public/upstream continuation bindings.
- [x] Centralize grants and same-account pre-publication transport recovery.
## 4. Affinity pool and admission
- [x] Enforce reserve on ordinary warm reacquisition with recovery exception.
- [x] Introduce seven-lane pooling, specific alias fencing and generation-independent queues.
- [x] Implement safe age rotation and resource accounting.
## 5. Verification and retirement
- [x] Remove replaced HTTP ownership/retry paths.
- [x] Run regression suite, pinned Bifrost acceptance and Astra/max review.
- [x] Prepare reviewable release and rollback artifacts; canary requires deployed candidate.
