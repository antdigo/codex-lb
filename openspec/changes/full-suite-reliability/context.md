# Context

Native body probes previously expired during cold response-head setup; tests now separate phases without changing production timeouts or retry policy. PostgreSQL reconciliation can preserve only an identity actually observed before row deletion commits; the opposite ordering must discard an unprovable owner, never guess by email.

A lazy closed dialog previously suspended the whole settings route. A local boundary keeps controls visible. Test-only QueryClient ownership and reader fences attribute asynchronous work to the test that created it.

Manual full CI is the acceptance path for browser and Helm/Kind checks unavailable in the contained local environment. Dispatching it requires publishing a concrete candidate ref separately. No service activation or production storage access is part of this change.

Example: a full-resend stale-owner failure observed before any SSE frame returns a JSON 502; after a keepalive commits HTTP 200 the same fence is carried by response.failed, with no replacement dispatch.

The final full backend run exposed two cancellation ownership gaps. A heartbeat interrupted during SQLite UPDATE RETURNING could retain its unconsumed cursor and writer lock. The heartbeat now owns and awaits the current renewal transaction through session cleanup before propagating cancellation, including a simultaneous renewal error. No lease duration, retry policy or schema changes are needed.

A completed startup probe is not a transfer to the HTTP delivery owner. Cancellation before that transfer must close the original service iterator explicitly, on Responses and Chat Completions routes, rather than depend on generator collection. The Responses regression holds a replacement reader open and proves operation authority closes before reader cleanup can proceed.

The native stalled-HEAD probe warms the actual worker HTTP path before starting its unchanged 150ms HEAD timeout. Cold helper/client startup can legitimately expire without the origin receiving a request; that is distinct from the stalled wire-HEAD failure this test asserts. Body timeout and truncation probes retain their separate phase checks.

The sequential downstream-WebSocket reuse fixture previously preloaded its first response and released its second response on the first send. That permitted future frames before the second response.create and made connection/owner assertions depend on scheduling. Both responses now use one batch per actual send. The test retains immediate follow-up coverage and adds an explicit reader-idle boundary. The generic fake still supports preloaded frames for deliberate stale/unsolicited-event tests; an AST audit found this was the only nonempty-initial/nonempty-first-deferred configuration in the module.

Shared terminal cleanup deadline tests previously sampled host elapsed time against 150ms and scheduled a rescue release at 250ms. Scheduler stalls could change their outcome without changing cleanup ownership. They now use the existing injected VirtualClock/VirtualScheduler, with a module-local shutdown clock replacement, and assert pending at 49ms and completion at the original 50ms deadline despite a longer one-second caller budget. Child work stays uncancelled until explicit release; no production timeout changes are involved.

Final V6 completed with 15426 passing cases and one host-throughput assertion failure: 100 concurrent account selections took 1.790s against a 1.25s wall-clock ceiling, while the isolated control passed. Replace this timing proxy with a 100-party blocked-persistence barrier, verify the runtime lock remains free, and always drain all selection tasks. A serialized selection mutation must fail that barrier. This proves nonserialized persistence rather than claiming an unmeasured host throughput SLA.

The source SSE keepalive test also fails when a deterministic250ms host stall makes its200ms outer wait cancel a correct10ms heartbeat. An opt-in shared fixture now injects VirtualScheduler through the existing shared-future wait seam, preserving the real SSE injector and all protocol normalization. Six heartbeat tests use explicit upstream events and owned read/iterator cleanup; timed heartbeats are pending at9ms and ready at10ms, while initial native frames remain immediate. Disabled-injector and host-stall controls distinguish actual missing heartbeats from host latency. This changes no production timer or API.
