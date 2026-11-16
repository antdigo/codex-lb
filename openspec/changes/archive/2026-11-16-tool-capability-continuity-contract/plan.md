# Tool capability and canonical request contract implementation plan

> **For agentic workers:** Use superpowers:executing-plans for inline implementation in the existing isolated worktree.

**Goal:** Correct the reviewed classes of capability loss, inherited authority and avoidable error-induced cache misses.
**Architecture:** Shared pure tool/input contracts, authoritative request-local independence and separate canonical/wire attempt snapshots; existing lifecycle controller remains the effect owner.
**Tech Stack:** Python3.13, pytest, early Linux Landlock isolation, pinned Bifrost2.5.1.
**Spec:** `design.md` and `specs/responses-api-compat/spec.md` in this change.

## Global Constraints
- No production service/config/database/Git mutations or live model calls.
- App tests require `-c pyproject.toml -p tests.isolated_bootstrap` before app imports.
- Preserve original deadline, central replay quota, publication and account/API/file/continuation fences.
- Previously prepared9a candidate stays held; no rewriting sealed runtimes.

## Review Focus
- Client/hosted/omitted execution and unknown loaded tool definitions remain distinguishable.
- All parallel call families must be represented or durable completeness is unknown.
- Broad session headers and promoted physical lanes do not confer request continuation authority.
- Prepared image/file data, tools, native ciphertext and ordering survive canonical retries.
- Long/raw suffix-like cache keys are transformed exactly once without extra retries/prewarm.

## Task 1: Shared capability contract
- [x] Add projection and real owner-recovery RED cases retaining definitions present only in discovery history.
- [x] Add mixed streamed/terminal-only pending-client manifest partial-suffix RED controls.
- [x] Extract a pure shared input/tool contract; consume it from native validation, portability and durable manifest paths; keep unsupported transfer fail-closed.
- [x] Run complete input/projection/manifest and recovery tests GREEN.

## Task 2: Request independence
- [x] Add warm broad-header/backend and promoted-lane sibling-context RED cases, with explicit continuation positive controls.
- [x] Apply existing request-local independence at final anchoring/preparation, without duplicating persistent state or introducing retries.
- [x] Verify whole affected route/pool suites GREEN.

## Task 3: Canonical attempts and cache
- [x] Add same-account isolated opaque replay RED cases, including long/suffix-like raw keys and shared-mode controls.
- [x] Retain explicit pre-egress canonical snapshot separately from final wire snapshot; derive retry egress from canonical snapshot once.
- [x] Verify tools/prepared input preservation, safe publication, original quota/deadline and current account scoping.
- [x] Integrate separate cache-miss report with minimal conditional measurement and prevention gates.

## Task 4: Delivery
- [x] Run affected complete suites, pinned Bifrost, static gates and address independent review findings.
- [x] Strictly validate/sync/archive specs and seal a separate verified source/runtime with code-only activation/rollback.
- [x] Provide review evidence, limits and concrete manual installation commands; no activation or Git integration.
