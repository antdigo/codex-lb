# HTTP bridge execution isolation implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans task-by-task. Steps use checkbox syntax.

**Goal:** A waiting HTTP bridge request cannot time out an already executing response.
**Architecture:** One independent semaphore on each existing bridge, with a nullable ownership handle on the request. Acquire before create admission, retain through safe same-session recovery, release at fenced terminal/cleanup boundaries. Preserve account/create admission and warm-cache reuse.
**Tech Stack:** Python3.13, asyncio/anyio, existing FastAPI integration and bridge tests.
**Spec:** specs/responses-api-compat/spec.md and context.md in this change.

## Global Constraints
User explicitly authorized autonomous implementation and opted out of design questions. Use the existing linked worktree, branch fix/http-bridge-execution-isolation from a3a7ed386. Production remains unchanged until a verified code-only runtime is activated. Preserve account ownership, exact payload/ciphertext/PCK, finite budgets, API-key settlement and existing accepted-output guards. No new dependencies, settings, migration, Bifrost changes or public-API stream_id assumptions. Tests use fake local upstreams; no paid provider suites. Independent fresh review before packaging; no cloud/merge-readiness claim.

## Review Focus
- Cancelling or timing out B must not release A's permit or account leases.
- Terminal bookkeeping failure/cancellation must fence admission before waking C.
- Prewarm timeout and pre-response reconnect must not strand or double-acquire execution.
- An already queued waiter must not send on a generation retired by owner cancellation.
- Independent sockets and canonical/noncanonical HTTP paths must keep their existing ownership and settlement.

### Task 1: Serialize upstream execution independently of response-created admission
Files: support.py (two bounded handles), http_bridge/execution.py (typed acquire/release helpers), service.py (shared normal/prewarm admission), http_bridge/request_submit.py (fenced cleanup), http_bridge/upstream_events.py (terminal owner), websocket/mixin.py (reader failure). Tests: existing public test_http_responses_bridge.py plus focused execution/cleanup units.
Interfaces: acquire_http_bridge_execution(request_state, session, *, scheduler, timeout_seconds) -> None; release_http_bridge_execution(request_state) -> None. Reentrant acquisition on the same owned gate is a no-op; ownership mismatch fails closed. Local timeout has an explicit error code and never enters connection-wide stale-gate failure. Remaining budget is recomputed after waiting. Execution does not release on response.created.
- [x] Write/run public RED: same-PCK A accepted and blocked, B exceeds local wait, A then completes, B has zero sends, later C succeeds/cache forwarded/same socket.
- [x] Implement the minimal independent admission/ownership contract and verify GREEN.
- [x] Add cancellation, FIFO success, distinct-socket independence, budget, prewarm timeout, recovery retention, reader failure, terminal-abort and retired-waiter tests; run each relevant RED before its fix.
- [x] Run focused and complete affected bridge tests, lint/type/architecture/strict canonical specs; inspect all failures. Perform independent whole-branch review and fix proven important findings.
- [x] Sync/archive only verified specs; commit locally, bundle source and prepare/verify the installed Python runtime and code-only activation transaction, retaining compatible DB and rollback.

## Verification and review

1595 affected unit tests and266 integration tests pass; lint, architecture and type checks pass. Eight installed Python transport chain scenarios pass against an isolated official Bifrost and strict Claude Code parser. Independent whole-source review identified terminal-detach ownership and global admission deadline gaps; both were demonstrated RED and fixed GREEN (47 focused lifecycle/budget tests pass). Canonical specs validate67/67; the new change validates strictly. Strict whole-repository validation has unchanged baseline warnings about existing long requirement paragraphs. Source is retained on the incident branch; runtime activation is a separate credentialed operator step.
