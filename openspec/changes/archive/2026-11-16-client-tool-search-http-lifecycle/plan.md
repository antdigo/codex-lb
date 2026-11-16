# Client tool-search HTTP lifecycle implementation plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task by task in the existing isolated worktree.

**Goal:** Support complete client tool-search histories in the accepted HTTP operation architecture.
**Architecture:** Extend the shared native-input proof and client-work capability. Reuse existing lanes, deadline, ownership and replay controller; never replace these with adapter retries.
**Tech Stack:** Python 3.13, pytest, Linux Landlock, pinned Bifrost 2.5.1.
**Spec:** `design.md` and `specs/responses-api-compat/spec.md` in this change.

## Global Constraints

- No live runtime/service/config/database mutation, provider calls, migrations or dependencies.
- Every app test starts with `-c pyproject.toml -p tests.isolated_bootstrap`; optional Bifrost receives a read-only exact binary grant and disposable config.
- Preserve existing source changes and sealed installed artifacts. No commit, push or merge.

## Review Focus

- Mixed function/search identifiers must not satisfy each other's pending outputs.
- A client search output must not load hosted tools into a purported client-only replay.
- Native arguments, ciphertext and cache keys remain byte-for-byte intact.
- Concurrent retired waiters share a replacement while keeping independent grants and original deadlines.
- Publication, cancellation, expiry and repeated retirement must never dispatch a duplicate attempt.

## Task 1: Regressions

- [x] Extend `tests/integration/test_bifrost_http_operation.py` with an actual synthetic Anthropic ToolSearch history, assert installed Bifrost's native HTTP shape, observe missing replay RED.
- [x] Parametrize `tests/integration/test_http_bridge_retired_execution.py` for complete client search and add malformed/hosted/cross-kind proof controls to `tests/unit/test_http_bridge_connection_lifetime.py`; run selected cases RED.

## Task 2: Shared capability

- [x] In `connection_lifetime.py`, recognize explicitly client-executed complete search pairs and expose one client-only tool-definition predicate. Preserve all item data and existing function/opaque checks.
- [x] In `http_operation.py`, `streaming.py`, `request_submit.py` and their direct tests rename the internal capability to `client_tools_only` and initialize it with that predicate; retain controller grants and deadlines unchanged.
- [x] Run changed unit/route tests and pinned-Bifrost acceptance GREEN, including original negative controls.

## Task 3: Verify and deliver

- [x] Run affected complete suites and architecture/static checks, strict specs and independent code review.
- [x] Freeze exact source and build a separate audited runtime with unchanged DB/locks/native dependencies; verify operator code-only activation/rollback with fake operations.
- [x] Seal evidence and provide the concrete activation and rollback commands. Do not activate.
