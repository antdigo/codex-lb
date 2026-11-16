# Post-refresh bridge retirement verification

## Root cause and boundary

The canonical registry reuse check precedes an awaited durable lease refresh. An older accepted response can detach while that await is suspended, setting retirement without taking the registry lock. Before this change lookup returned the condemned session and public submission raised the exact retiring 502 without dispatch. The final submit fence is correct; the lost safe opportunity is in this earlier lookup window.

The two recorded pre-cutover failures show compatible reuse, retiring rejection, close and old-request cancellation timestamps. The journals do not identify the precise flag assignment, so they support this path without proving the entire historical chain. The earlier shared-socket routing defect can explain the old long-lived owners, but this report does not equate all retiring rejections with that defect.

## RED and GREEN

- Public route RED: `.venv/bin/python -m pytest -q tests/integration/test_http_responses_bridge.py -k old_request_detaches_during_refresh --maxfail=1 --tb=short` failed with `502 != 200` on base source (1 failed, 196 deselected, 6.58 seconds).
- Review boundary RED: the same public case with a visible accepted sibling failed because the first candidate replacement implementation closed that sibling's session (1 failed, 200 deselected, 5.51 seconds). The final implementation routes refresh retirement through the existing drain-aware detach.
- Final focused command: `.venv/bin/python -m pytest -q tests/integration/test_http_responses_bridge.py -k 'old_request_detaches_during_refresh or reused_unanchored_refresh_reserves_canonical_handoff' --maxfail=1 --tb=short` passed all 14 cases in 26.73 seconds.
- Independent read-only reviewer passed 20 cases including file/required-owner, epoch, cancellation and handoff controls in 38.09 seconds and found no Critical/Important production issue. The normative scope was narrowed after the review's documentation comment.

The matrix proves one replacement send/create attempt with zero replay count, unchanged input, zero continuity operation rows on the unanchored path, one finalized/released reservation for a limited API key, no owner selection or dispatch on rejected explicit anchors under soft or hard affinity, no sibling socket close, and preserved reserved unanchored handoff authority.

## Limits and request-path cost

The final submit retirement fence remains intact, so a retirement after lookup may still fail closed. The separate previous-response-alias refresh branch is outside this change. No live model call, production data write, runtime activation, push or merge was performed by this implementation task. There is no database, setting or native protocol change and no native rebuild requirement.

An isolated actual-helper benchmark (durable I/O/logging stubbed, 11 interleaved samples of 100,000 calls) measured median CPU time 1,273.96 to 1,382.48 ns per call (+108.52 ns, +8.52%). This is not end-to-end latency. The added state is one boolean return and a flag read, with no retained buffers or cache.

## Broader checks

- `.venv/bin/python -m pytest tests/unit/test_proxy_http_bridge.py tests/unit/test_proxy_utils.py tests/unit/test_proxy_api_responses_contract.py tests/unit/test_check_proxy_architecture.py -q -n 2 --maxfail=1 --tb=short --timeout=90 --timeout-method=thread`: **2626 passed**, 127.98 seconds.
- Full `make lint`: **passed**, including cancellation/timing/settings/migration checks, Ruff and formatting for 1306 files. Architecture remains within the existing 2436-line bridge-mixin limit.
- Full `ty check`: **passed**.
- Strict change validation and all 67 canonical specs: **passed** before archival.

The final two production files and the integration test match the independent review's recorded SHA-256 values. No production/test bytes changed after that review.

- Full affected integration module: `.venv/bin/python -m pytest tests/integration/test_http_responses_bridge.py -q -n 2 --maxfail=1 --tb=short --timeout=90 --timeout-method=thread`: **209 passed**, 310.62 seconds. All collected cases completed on the final reviewed production/test bytes.
