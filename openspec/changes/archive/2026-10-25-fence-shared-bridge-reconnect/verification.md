# Shared bridge and accepted terminal verification

This report covers `fence-shared-bridge-reconnect`, `reject-ambiguous-created-output`, and `recover-accepted-terminal-output`.

## Production changes

- `12df04a1f`: pending-owner reconnect fencing and ambiguous anonymous-output rejection.
- `6b95e83c8`: accepted terminal lifecycle recovery and public post-terminal suppression.
- `1398da38e`: lifecycle serialization of prewarm timeout recovery.

Later commits update integration expectations only. All final integration passes used identical production source, verified with `git diff 1398da38e -- app`.

## Regression evidence

Before the fixes, real bridge-reader regressions replayed one request while an accepted/draining sibling remained pending. Three multiple-created-owner cases attributed ambiguous frames by visibility/draining/pre-created fallback. Accepted creation followed by full terminal output emitted text before item creation and omitted streamed function arguments. Duplicate terminal and late argument frames could replay output. Each failure was observed before its corresponding fix.

Independent review additionally reproduced reconnect overtaking a plain submitter between its closed check and pending publication. The repository regression drives the actual prewarm caller, real submit/reconnect logic, production fast locks and a deterministic scheduler deadline. It failed before lifecycle serialization and passed afterward; all 41 selected prewarm/cancellation/sole-recovery controls also passed.

## Final checks

- Complete affected unit modules: **2626 passed** (`test_proxy_http_bridge.py`, `test_proxy_utils.py`, `test_proxy_api_responses_contract.py`, `test_check_proxy_architecture.py`; two workers).
- Six affected integration modules: **607/607 collected cases passed**, including all 196 HTTP bridge and 160 WebSocket cases, public Responses, extended API, warmup and local affinity recovery. Coverage is the exact union of 130 recorded passes and 477 resumed cases on identical production source. Every collected node id is accounted for; there are no missing cases.
- Actual OpenAI SDK streaming consumption uses a local `httpx.MockTransport`; owned message text and function-call arguments are delivered exactly once.
- Full `make lint`, full `ty check`, and strict OpenSpec checks passed. The main HTTP bridge mixin remains within its existing 2436-line ratchet.
- Two captured output-free accepted streams recovered from four events to complete 14- and 16-event lifecycles, preserving function arguments exactly and retaining one creation.

Commands use the repository's frozen virtual environment, local test databases and mocked upstreams. No live model generation or production database write was required.

## Limits and cost

The unrelated all-project run was interrupted after 419 passes and 232 skips. A broader unit attempt reached 7941 passes and 96 skips before an outdated strict reconnect double rejected the new warmup-ownership argument; that double was corrected and is covered by the final 2626-case affected unit run. A complete all-project/full-unit green run and cloud CI are not claimed here.

The normalizer benchmark is documented in the owning capability context. It adds constant per-stream state, no growing buffer or long-lived cache; the repeat measurement showed +24 peak traced bytes and about +0.85% CPU for an ordinary 14-event Python normalizer stream, with a loaded run showing +5.4%. These are not end-to-end gateway latency or allocation-count measurements. Recovery checks add no per-frame reconnect work, and the anonymous matcher reuses its existing created-owner list.

The implementation contains no configuration, schema or native protocol changes. It requires no native helper rebuild. Runtime activation and live upstream transport diagnosis are separate operational work; no deployment, push or merge was performed by this implementation task.
