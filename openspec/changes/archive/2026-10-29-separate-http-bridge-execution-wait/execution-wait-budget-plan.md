# Separate HTTP bridge wait budgets implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans, native execution with one fresh final review.

**Goal:** Healthy concurrent requests outliving the ten-second create budget can wait without aborting each other.
**Architecture:** Use the existing bounded pre-response policy for ordinary execution waiting; start one session/global create deadline after ownership; retain the original outer request deadline. Prewarm remains short.
**Tech Stack:** Python 3.13, existing virtual scheduler, real FastAPI public-route tests with controlled local upstreams.
**Spec:** specs/responses-api-compat/spec.md and context.md in this change.

## Global Constraints
Standing user authorization permits autonomous fixing without design questions. Existing linked worktree at bb7b8975 is isolated. Preserve socket/account/input/PCK/ciphertext, queue limits, cancellation/settlement, and raw WebSocket behavior. No dependencies, settings, migrations, Bifrost changes or speculative transport replay. Production is unchanged until verified activation. No live paid-model test suite. No new branch or publication is needed for this local fix.

## Review Focus
- Retained earlier deadlines after retry must bound both wait phases.
- Waiting/cancellation cannot release a sibling's permit or bypass retirement.
- Shared global create admission cannot restart its deadline.
- Optional prewarm cannot inherit a long ordinary execution wait.
- Public timeout/cancellation controls and API-key reservations remain correct.

### Task 1: Separate wait budgets and verify public behavior
Files: app/modules/proxy/service.py; app/modules/proxy/_service/http_bridge/execution.py; tests/unit/test_http_bridge_execution.py; tests/integration/test_http_responses_bridge.py; the owning OpenSpec spec/context.
Interfaces: new internal acquire_http_bridge_execution_before_create returns an absolute create deadline; existing acquire_http_bridge_execution(state, session, scheduler, timeout_seconds); existing _http_bridge_eventless_budget_seconds and _http_bridge_admission_timeout_seconds. No new public interface.
- [x] Add a virtual regression: an ordinary waiter remains pending past ten seconds and acquires after 25 seconds; optional prewarm still times out at ten.
- [x] Change the former shared-budget test to assert a full ten-second create budget after six seconds of execution waiting; preserve the retained ten-second request-deadline control.
- [x] Run regressions RED against bb7b8975.
- [x] In the HTTP branch of shared admission, acquire ordinary execution with the existing pre-response policy clamped to request deadline, then start the short create deadline; preserve prewarm's short wait.
- [x] Run regressions GREEN and add public long-wait variants across v1/backend and auth. Give the intentional timeout waiter its own short request deadline, preserving its active sibling's budget.
- [x] Run relevant unit/integration checks and project checks, then dispatch independent multidimensional reviews with gpt-6-astra at maximum effort as previously requested. Fix all confirmed findings with RED→GREEN and repeat review until no findings remain, as explicitly requested by the user.
- [x] Sync/archive verified specs, seal a versioned runtime, verify isolated chain and prepare activation/rollback. A sudo password may require a final user-run command.
