# HTTP transport boundary fixes and installable update
> For agentic workers: use superpowers:subagent-driven-development for one coordinated application fix, with root-owned test execution and release preparation.
**Goal:** Close both confirmed fresh-review findings and provide a verified installation command.
**Architecture:** Keep the approved operation controller, publication gate, seven-lane scheduler and ownership fences. Repair the missing decisions at shared transport boundaries; reuse the proven code-only deployment transaction.
**Tech stack:** Python>=3.13, existing pytest/virtual-time fixtures, systemd source runtime.
**Spec:** design.md, specs/responses-api-compat/spec.md, canonical responses-api-compat and proxy-admission-control.

## Global Constraints
- Python >=3.13; typed interfaces; no schema/dependency/settings drift.
- Account cap8/reserve1, seven lanes; existing queue and all-generation accounting unchanged.
- No retry after readable publication or ambiguous send; ordinary replay quota and original absolute deadline remain shared.
- Exact API/account/lane/generation/owner continuation fences remain mandatory.
- No production prompts/tokens in tests/logs; no real upstream model calls.
- No branches, commits, pushes, merges or automatic service activation.
- ROOT executes ALL pytest through the fail-closed isolated launcher; workers edit/read/static only. A command approval is never evidence of data isolation.
- Before pytest/application imports, use a temporary home/database and kernel filesystem containment excluding production data, config and backups. Verify denial using a disposable canary; never attempt production writes in a protection test.
- Destructive fixture setup MUST validate the actual engine/connection against the intended test URL before connecting/resetting; a preimported mismatched engine must fail closed. Regression tests use synthetic sentinel databases only.
- Existing source worktree is isolated; retain old sealed candidate unchanged as baseline.

## Review Focus
- Generic code-less 429 burst and explicit usage-limit refusals after handoff: retry only under matching controller grant.
- Retry quota exhausted or publication committed: no extra upstream send.
- Recent WS failure marker and explicit/config/payload bypass with projected ID: verify owner or refuse before sending.
- Missing/stale/foreign API binding and process-local map loss: no raw leak of public ID.
- Ordinary provider IDs and direct WebSocket/raw paths retain compatibility; installer rollback retains live tokens.

## Tasks
0. Verify the isolated launcher and add fail-closed destructive-fixture guards after the database incident. Independent review and disposable canary/sentinel tests precede application regression runs.
1. Add permanent route-level RED tests for R1/R2 and adjacent negative cases; root runs them before app edits.
2. Repair R1 first, root targeted GREEN, then R2 and root targeted GREEN. Inspect shared classification/routing causes, not exception-message patches.
3. Run affected raw/bridge/continuation/ownership suites, repository unit/simulation command and pinned Bifrost acceptance; resume the fresh reviewer for scoped fix review.
4. Prepare isolated runtime/operator scripts/identity manifests and test deploy transaction using fake operations and temporary files; never activate while preparing.
5. Verify/reseal source and runtime after clean review, archive this change, provide exact activation/rollback commands and canary instructions.
