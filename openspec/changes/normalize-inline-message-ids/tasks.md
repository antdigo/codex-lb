# Inline message IDs Implementation Plan

> Execute inline with superpowers:executing-plans and test-first verification.

**Goal:** Fix the reproduced reused-session message-ID rejection while preserving transport and ownership contracts.
**Architecture:** Generalize the existing wire-ID traversal and existing one-bit ownership evidence; retain all bypasses and reconstruction paths.
**Tech Stack:** Python 3.13, Pydantic, pytest, FastAPI.
**Spec:** specs/responses-api-compat/spec.md and design.md in this change.

## Global Constraints

- Only completed assistant output_text message shapes qualify; preserve encrypted/reference/unknown/incomplete shapes.
- No new settings, dependencies, migration, session cache, content retention, retry or account-switch policy.
- Preserve caller/forwarding/replay-safety views and direct client WebSocket/Lite IDs.
- Keep private payloads and runtime deployment files outside the public PR.

## Review Focus

- Caller mutation and existing content coercion on completed inline text.
- Empty, incomplete, encrypted, reference, and unknown message shapes.
- Direct WebSocket initial/fresh replay and metadata-only Lite continuation.
- Original ownership through anchor trim, fresh preparation, auth and account switch.
- Actual affected-session replay and bounded serializer cost.

## Task 1: Wire normalization and ownership

Files: app/core/openai/requests.py; existing _service serialization/evidence call sites; tests/unit/test_openai_requests.py; tests/unit/test_proxy_http_bridge.py; tests/integration/test_proxy_responses.py.

- [x] Add failing literal-fixture tests for message-ID normalization, public routes, and original ownership after trim/reprepare.
- [x] Run the new regressions and confirm the missing normalization/evidence failures.
- [x] Generalize the existing traversal, transport opt-out, and one-bit evidence flag.
- [x] Run focused transport/replay/compact suites and static checks; record baseline failures separately.
- [x] Measure before/after serialization using the existing deployed version as baseline.

## Task 2: Deployment and publication

- [x] Validate this OpenSpec delta strictly and review the diff independently.
- [x] Verify the patched implementation against the captured failing payload in isolated runtime.
- [x] Prepare and independently review verified deployment/rollback with bounded readiness and preserved rollback targets.
- [ ] Activate the privileged service and verify the affected route after restart; local sudo currently requires an interactive password.
- [x] Commit and update the existing fork PR with exact tests, observations and remaining limits.
