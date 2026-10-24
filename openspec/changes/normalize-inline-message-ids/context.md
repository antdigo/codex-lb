## Reproduction

The same full request in a fresh diagnostic namespace completed with original IDs and with reminted message IDs. On the original affected prompt-cache key, the controlled comparison was:

| Variant | Result |
| --- | --- |
| Original IDs | unsupported_parameter / persisted-item lookup |
| Only six message IDs omitted | completed; 124,813 input, 121,344 cached, 362 output |
| Original IDs restored | same persisted-item lookup error |

The encrypted reasoning bytes/IDs, function-call call_id values, text, tools, instructions, and effort were unchanged. No returned tool was executed. This demonstrates the missing message-ID compatibility case in the affected session; it does not establish that all IDs always fail or eliminate every possible backend error.

## Operational scope

The patch stays in the subscription adapter. Bifrost remains the official release. The original function-call-ID normalization and instruction-prefix fix remain active; direct WebSocket and Lite retain their existing wire contracts.

## Verification

TDD reproduced 14 missing-normalization/ownership failures before the production change, followed by 42 passing new/extended cases. The broader affected Responses/chat/HTTP bridge/WebSocket/replay/compact selection passed 1,339 cases. Exactly two previously reproduced clean-v1.24 baseline failures were excluded from that selection: repeated_401_after_refresh_fails_over (fake refresh HTTP 403) and fails_closed_before_file_affinity_when_previous_response_owner_misses (fixture missing file_account_pins). Whole-project results are recorded separately in the PR.

The actual patched serializer, before changing the running service, was also tested on the still-affected bridge: baseline rejected; patched body completed with 124,813 input / 124,672 cached / 495 output; baseline rejected again. An offline equality check proved the normalized bodies differed only by the six message IDs. All eight candidate production modules match the reviewed source; isolated installed-package import and real HTTP preparation/ownership checks passed.

## Serialization cost

Paired measurements under concurrent test load (microseconds per wire serialization): common no-ID input 144.06 -> 97.10; one completed message 85.82 -> 86.33; captured 219-item request 12,529.95 -> 13,184.97. Timing ranges were wide and overlapping (large request 11,758.34–14,361.45 vs 9,698.66–19,904.20), so these are observations rather than a speedup or latency guarantee. Peak traced memory was unchanged for the small shapes and 378,743 -> 378,807 bytes for the large request (+64 bytes).

No additional retained state was introduced: the existing ownership boolean was generalized. The existing ID traversal is reused; there is no extra input traversal or new session collection.
