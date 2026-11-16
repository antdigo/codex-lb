# Exact repeat locality implementation plan

> Execute inline using superpowers:executing-plans in the existing isolated worktree; no commits/push/activation.

**Goal:** Close the confirmed advisory-locality and normative-discovery contradictions.
**Architecture:** Existing fingerprint, local advisory comparison, unchanged strict continuation authority.
**Tech Stack:** Python3.13, protected pytest/Landlock/tempSQLite, pinned Bifrost2.5.1, OpenSpec1.11.
**Spec:** design.md and specs/responses-api-compat/spec.md.

## Global Constraints
- All app tests use -c pyproject.toml -p tests.isolated_bootstrap; no production DB/config/service or provider calls.
- Keep old sealed candidates, app/db/native/dependencies, pool width and retry permissions unchanged.
- Existing native execution method and authorization persist; no test permission questions.

## Review Focus
- Equal complete input must prefer its actually distinct warm lane (two real routes).
- Strict extensions retain preference; mismatched/absent prefix proof cannot earn locality.
- Advisory equality must not authorize strict continuation trimming (existing helper control).
- Scope/queue/cancellation controls remain intact (whole affinity-pool unit suite).
- Portable client discovery remains preserved; unknown/hosted discovery still refuses account-neutral recovery (existing whole capability/replay tests).

## Tasks
1. Promote the confirmed two-route/two-lane probe and proof controls; run RED before app changes. Modify only pool_routing.py advisory comparison; GREEN and affected suites. Correct old canonical projection wording and its delta.
2. Verify affected pool, snapshot, native/discovery, policy and actual Bifrost/ops/static paths; fresh bounded review. Sync/archive verified spec, prepare/audit/seal a separate source/runtime and manual code-only install/rollback. No Git integration or live activation.
