# HTTP bridge operation lifecycle implementation plan

Spec: design.md and specs/ in this change. Execute tasks.md in order; integrate admission independently while core ownership is extracted. No branches/commits/pushes without explicit authorization; existing isolated worktree is used.

## Global Constraints
Python >=3.13, typed interfaces, no schema/dependency drift, no private production prompts/tokens in tests/logs, no automatic deployment. Use RED/GREEN tests, preserve ownership/pins/settlement and direct WebSocket compatibility.

## Review Focus
Wrong-lane continuation, reserve bypass on warm reuse, stale generation publication, cancellation during settlement, duplicate output after opaque reasoning replay.
