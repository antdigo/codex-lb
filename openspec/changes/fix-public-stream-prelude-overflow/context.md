## Design

The public SSE normalizer must wait for an owning response envelope even after
its pre-created replay buffer reaches 64 payloads. Clear the complete unowned
prelude on overflow, stop retaining further pre-created payloads, and continue
reading within the existing upstream request budget. Never attach orphan text,
function arguments or item IDs to the eventual owned response.

If the first owned envelope is completed/incomplete, render only its owned
output as SDK-compatible lifecycle events before the terminal event. Preserve
message/refusal parts, function/custom-call arguments, reasoning summaries and
encrypted state exactly once. Real created envelopes resume normal streaming.

## Boundaries

Keep native Codex failure lifecycle, vendor/source passthrough, true EOF and
cancellation semantics. No new retries, cache keys, accounts, settings,
dependencies or migrations. Retention remains bounded at 64 pre-created replay
payloads and empty after overflow; this is not a bound on other existing stream
accumulators. Terminal reconstruction is a generator used only on the abnormal
recovery path.

## Validation

Synthetic regressions reproduce premature failure on current main, including
the real OpenAI SDK against both canonical and trailing-slash public routes.
Current local checks, full-suite results and independent review are recorded in
the PR. Private requests and host-specific runtime artifacts are excluded.
