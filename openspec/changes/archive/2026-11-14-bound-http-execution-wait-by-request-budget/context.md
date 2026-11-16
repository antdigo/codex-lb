# HTTP execution wait budget implementation plan

> For agentic workers: execute this single task inline using superpowers:executing-plans and TDD; obtain one independent whole-change review at the end.

**Goal:** Let an ordinary queued request outlive the unrelated five-minute pre-response watchdog while preserving bounded execution ownership.
**Architecture:** Select min(bridge-request budget, stream-idle budget), then clamp to the original absolute deadline. Keep existing ownership and cleanup paths.
**Tech Stack:** Python 3.13, asyncio, pytest virtual time and public ASGI routes.
**Spec:** specs/responses-api-compat/spec.md in this change; canonical openspec/specs/responses-api-compat/spec.md.

## Global constraints
One execution owner per physical socket; existing queue/account/continuity limits; no new settings or transport changes; original absolute deadline is retained across retry; optional prewarm retains short admission.

## Review focus
Short stream-idle settings, retained retry deadlines, cancelled waiters, retired generations and API-key settlement must keep their current behavior; existing regression cases cover these.

## Task 1: Separate execution waiting from the pre-response watchdog
- [ ] Add test_ordinary_execution_wait_outlives_pre_response_watchdog to tests/unit/test_http_bridge_execution.py: advance virtual time past 300 seconds, complete owner at 330, acquire exactly one execution permit.
- [ ] Extend test_public_http_bridge_waiter_isolated_from_streaming_owner in tests/integration/test_http_responses_bridge.py with past_eventless: cross a shortened watchdog with sufficient request/idle budget, then complete both streams on one socket; cover both routes and authentication variants.
- [ ] Run those cases; expected RED because ordinary execution uses the 300-second watchdog.
- [ ] In app/modules/proxy/_service/http_bridge/execution.py, change only ordinary timeout selection; expected GREEN plus existing short-idle/deadline/cancellation/prewarm cases.
- [ ] Verify related public bridge, execution, admission, cancellation and rotation tests plus lint/type/architecture and strict OpenSpec checks. Sync/archive after verification.
- [ ] Review the bounded diff on gpt-6-astra/max, record findings, commit under the existing session authorization to publish the codex-lb fixes, and prepare a pinned reversible code-only installer.

## Execution ledger
Baseline: 29 execution tests passed, with the existing Starlette deprecation warning.
Pre-flight: one task; no shared task interfaces. Use the existing isolated worktree and branch; do not create another checkout.
Ruling: OpenSpec context holds the plan because repository instructions make OpenSpec the SSOT for behavior documentation; a separate docs/superpowers copy would duplicate it.

Regression RED: all five new cases failed on the pre-fix implementation; GREEN: all five passed after timeout selection changed.
Baseline control: the old owner_cancel expectation also fails on unchanged HEAD 8baa89b5 (KeyError, completed response instead of error).
Ruling: correct that stale public-test expectation to require one successful same-account replacement after owner cancellation, as the existing retired-execution replacement specification already requires; no cancellation/recovery production behavior changes.

Broader first run: 1446 passed; test_v1_responses_http_bridge_idle_recovery_hands_reader_to_replacement timed out. A control with unchanged HEAD execution code also timed out.
Ruling: shorten only the downstream eventless keepalive counter in that existing idle-recovery fixture; its old keepalive-count patch no longer controls the deadline. Keep upstream acknowledgement watchdogs unchanged and assert restart_reader=True, restoring the intended downstream reader handoff without production changes.
Independent gpt-6-astra/max review approved production change and initial test diff, independently 58 passed.

Review correction: lowering the shared watchdog in the idle-recovery fixture exercised upstream ack recovery rather than downstream reader handoff. Added restart_reader=True contract assertion; observed RED with the wrong timer. Shorten only the downstream eventless keepalive counter and keep the real recovery/reader paths. Await owned persistence before fixture teardown.
The four-worker exploratory run was interrupted (1183 passed) after two existing durable-owner unit cases encountered a missing accounts table; final gate uses the established two-worker configuration. No related production path was changed.

Review Important resolved: restart_reader contract assertion reproduced the wrong shared-watchdog fixture path (RED); downstream-only timer seam plus real retry wrapper passes (GREEN, 1 case in 4.83 seconds). Final standard two-worker related suite: 1447 passed in 354.59 seconds, with two existing Starlette deprecation warnings.

Final evidence: independent selected58 pass; related1447 pass; installer26 pass (+7 subtests); ruff, type, architecture, cancellation and timing checks pass. Candidate829 hashes and734 tracked app/config files verified; disposable clone69 tables unchanged; live service/database not modified. Runtime sealed at app revision7db826fb.
