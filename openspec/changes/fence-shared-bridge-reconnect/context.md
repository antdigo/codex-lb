# Shared bridge reconnect investigation

## Evidence

On 2026-10-25, production logs contain 760 warnings for upstream events matching no pending request. All report a prompt-cache bridge with two pending requests and no event response id. Output-item, content, reasoning, text, and function-argument events are affected. Two independently observed bursts immediately follow `retry_precreated` and `reconnect` (18:28:16–17 UTC and 18:53:23 UTC). Downstream evidence includes a created/in-progress pair, response metadata, and a completed envelope with full output; the intermediate sequence numbers are absent.

The retry path checked for sibling requests only when the selected retry candidate was itself accepted. A pre-created candidate could reconnect while an accepted or draining sibling remained tied to the previous physical socket. Helper and real-reader regressions reproduced that defect before the fix. Logs alone do not identify the exact historical pending request states. A separate change closes a visibility/draining fallback in anonymous matching; historical cross-consumer content disclosure has not been established.

## Plan and ownership

Keep the early pending guard in `app/modules/proxy/_service/http_bridge/request_submit.py` and fence the common reconnect entry through `_prepare_http_bridge_reconnect`. Exercise the bridge retry, upstream-reader, direct reconnect and prewarm paths in `tests/unit/test_proxy_http_bridge.py`. Never infer ownership from FIFO, most recent creation, timing, or shared prompt-cache locality. The existing reader failure funnel owns failed-socket settlement when recovery declines.

All reconnect call sites were audited: pre-created retry has the early guard; fresh-upstream recovery reaches reconnect from two closed-session submission paths; auth and security retries already check other-pending state in their event classifiers; prewarm timeout identifies its own disposable warmup. The common entry now protects each path before account selection or connection mutation, including a concurrent-owner change after a caller's earlier snapshot. The guard and sent-time reset live in the request-submit mixin to respect the repository's main-mixin line-count ratchet.

No native helper rebuild, database migration, external generation, service restart, or deployment is required to test this Python change. Existing per-request/session state suffices; it adds no retained memory or per-frame work.
