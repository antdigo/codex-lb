The existing HTTP-only API-key policy was tested before choosing this fix. Three real HTTP calls completed and replayed native encrypted reasoning, but cached_tokens were 0/0/0. A persistent-WebSocket positive control with the same first rendered input yielded 0/5760/5760 cached tokens (~97% on follow-ups). No live policy was changed. This supports preserving the persistent bridge for this backend; it is not a universal claim about HTTP prompt caching.

Physical age must be separate from last_used_at and reset on successful replacement. Account-bound reasoning is preserved verbatim and is not evidence of cross-account portability. A narrow original-item guard requires full native items and exactly paired function calls/outputs.

The new age branch must require zero literal pending, queued and admission owners, no handoff/reservation/close owner, and no reader processing. A reader can have popped a terminal request but still be persisting it. Existing idle pruning's weaker visible-count semantics must not be widened. If any proof is uncertain, keep the existing lifecycle; do not close owned work or invent retry.

The implementation retains two scalar fields per session (physical opening time and reader-processing ownership). A young-socket lookup only checks the registry and scalar age; it allocates no age capability and performs no new payload traversal. Original-item validation and call pairing occur only for an aged, unowned candidate. No history copies, new cache, or database lookup are added by the age policy.

The existing idle-prune method moves unchanged to the session registry mixin to preserve the architecture size ratchet. Its visible-pending semantics are not reused by the stronger age policy. A typed required-owner replacement error prevents outer raw-HTTP fallback while preserving the upstream error envelope.

Prompt-cache hits are upstream-dependent; replacing a socket does not guarantee cache retention or a deterministic warmup. The policy preserves the WebSocket transport and prompt-cache key.

The scope uses the Responses contract rather than durable or local history fingerprints: with no original or effective response/conversation/turn/session anchor, the complete original input defines the entire new context. A soft prompt-cache key does not supply implicit conversation state. Restricting the policy to non-Codex sessions, known full inline item shapes, exactly paired function calls and outputs, and the same account preserves that contract on a fresh socket. This also tolerates regenerated message identifiers without assuming cross-account ciphertext portability.

A native function output with a complete string body may retain status `incomplete` when a tool was cancelled. This is accepted without changing the status; incomplete calls and reference-only items remain rejected. The age path constructs no replay projection and sends the canonical prepared request unchanged. The risk boundary is the allowlist: accepting an unknown reference-dependent shape could lose context, so unknown shapes are conservatively skipped.

The required owner is carried as one request-state account-ID handle after request preparation. The central reconnect boundary enforces it even if ordinary soft preferences change. Higher-level reroutes that do not preserve that contract fail closed before another send; file-account flags retain their original meaning.

An age predecessor enters the existing planned synchronous close list. Capacity accounting can discount that specific committed close while preserving its resource ownership, and bounded cleanup finishes outside the registry lock before opening the replacement. This avoids both false exhaustion at a one-session cap and eviction of unrelated idle entries.

The 3000-second threshold leaves roughly 600 seconds before the observed one-hour upstream connection limit. A local operational sample of 114 successful Responses streams had p50 12.498 s, p95 53.015 s, p99 79.036 s and maximum 98.464 s. This supports the chosen headroom for that successful sample; it does not bound failed, ongoing or future generations, and an unusually long generation can still exceed the socket limit.

Detachment immediately installs the existing cancellation-safe background cleanup owner as well as retaining the predecessor in the planned-close list. If a later owner/ring lookup fails or the request is cancelled before that list is awaited, the detached socket still closes. Both paths share the existing single-flight resource-close task, so the socket and leases are released once.
