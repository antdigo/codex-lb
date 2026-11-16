# Bound HTTP execution waiting by its request budget

## Why
A Claude Code request at 2026-11-14 18:23:16 UTC waited behind an accepted
same-cache-key response and failed after 300 seconds. The predecessor completed
successfully after 330 seconds. Execution waiting currently reuses the unrelated
pre-response watchdog cap even though the request and stream-idle budgets are
7200 seconds.

## What Changes
Use the smaller of the configured bridge-request and stream-idle budgets for
ordinary execution waiting, additionally clamped to the retained absolute request
deadline. Keep short prewarm admission and all ownership/cleanup fences.

## Impact
Only local execution admission changes. No schema, dependencies, transport policy,
cache keys, socket rotation policy or accepted-response replay changes.
