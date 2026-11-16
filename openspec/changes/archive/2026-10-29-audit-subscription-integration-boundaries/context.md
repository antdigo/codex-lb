# Design context

The integration crosses three identities: client transcript item identity,
physical upstream bridge/account ownership, and public response ownership.
Inline lookup IDs may be removed only at effective classic transport boundaries;
call IDs, encrypted state, original input order and trusted incremental Lite
identity remain unchanged. HTTP/WS wire copies share input, so delayed classic
normalization must occur only after HTTP selection or handshake fallback.

An accepted response owns its public output, quota settlement and owner-cache
entries. Explicit nested response.id and top-level response_id evidence survives
core error normalization. Classic service rejects foreign evidence before tier,
continuity rewrites, usage, terminal settlement or owner publication. Source
streams receive an equivalent barrier before reasoning fragments can be grouped
or dropped. Event classification includes supported errors without a type field.
Local producers use the existing ParsedSseBlock provenance; untrusted JSON cannot
assert that a locally added ID is an upstream response owner. Reformatting keeps
that metadata and shares the immutable parsed payload. Local failures anchor to
the accepted response and retain their original error details and retry hint.

First-terminal recovery depends on whether any visible prelude was actually
replayed, covering absent, short indexed, overflow and hidden unindexed preludes.
Existing visible legacy text is not duplicated; foreign pre-created identity is
discarded before legacy replay. Reasoning never becomes assistant text. The first
terminal freezes public bookkeeping; later frames, transport-context cleanup
exceptions and vendor work cannot change its accepted outcome or usage. Missing
item/content/argument openings are repaired before owned item-done or terminal
snapshots, without replaying already visible output or accumulating delta text.
Lightweight lifecycle metadata is keyed by output and content indexes and grows
with item/part count, not chunk count. Native passthrough skips that state.

Sparse item announcements need a bounded ordering gate: the OpenAI SDK appends
item openings but indexes later content/argument events by output_index. Index 1
therefore cannot open before slot 0. Defer only the affected public output until
upstream or an owned terminal supplies preceding positions; never add synthetic
placeholder identities. Already valid interleaved streams remain immediate.
Storage is lazy and bounded by 64 events and 16 MiB of serialized UTF-8 SSE,
plus the parsed representation of those bounded payloads and small index metadata.
Pending output is freed at terminal failure, overflow, cancellation or completion.
Overflow or unresolved gaps fail explicitly rather than forwarding an invalid SDK
sequence. Only the sparse path may reserialize to keep sequence numbers monotonic.

Execution and create admission are distinct. Final ordinary and optional warmup
publication check retirement under pending_lock. Only the captured ordinary
handoff may use its authorized detached lane. Account identity is captured before
submit preparation, checked before leasing after execution, and checked again at
publication. An account change refuses the unsent waiter; its input is never
reprojected onto another account. Cancelled-only retirement keeps its existing
close/settlement policy. Once terminal processing claims a warmup, its consumer
may not release that execution permit while finalization is still running.

The unchanged queue limits bound retained requests, whose payload/task/reservation
lifetime can now reach the ordinary pre-response budget. Additional owner state
is fixed-size references/scalars. Same-owner fences use a fixed number of ID
comparisons; framing/parsing is linear in serialized bytes. Existing terminal
backfill sorts collected output indexes in O(I log I) for I items. New lifecycle
tracking uses constant-time indexed updates and O(items + parts) snapshot walks;
the ordering gate releases each bounded deferred event once.
Plain blocks gain a transient parsed carrier, cached carriers are reused. API
iterator owners close their children explicitly; keepalive readers are cancelled
and joined before those children. Response-level cleanup owns startup/service
handles independently of body-generator entry, so failed header writes and ASGI
disconnects cannot leave a hidden reader or reservation behind. Verbatim
bytes and original model inputs remain stable. No database, dependency, setting,
Bifrost binary or native protocol change is introduced.

Verification uses virtual clocks, real submit/recovery/terminal bookkeeping,
actual wire seams, real SDK accumulation, synthetic source bytes, and isolated
installed-runtime/WebSocket/official-gateway/CC-parser probes. Real upstream
cache effectiveness and network availability are separate observations.

## Measured processing cost

On the reviewed Python 3.13 candidate, an independent original-upstream-to-fork
microbenchmark used 71 input SSE blocks per request, alternating samples and the
same future-annotations mode. The public normalizer measured:

| Input shape | Before → after, microseconds/input block | JSON parses/request |
| --- | --- | --- |
| Canonical blocks with an existing parsed-payload cache | 12.019 → 22.152 | 0 → 0 |
| Plain canonical blocks | 42.817 → 43.903 | 142 → 71 |
| Plain data-only blocks requiring canonical framing | 53.670 → 58.721 | 142 → 71 |

Correctness checks add real per-block CPU cost; fewer parses compensate on plain
inputs. Valid fixture output bytes were identical. This cache is the local parsed
SSE payload cache: these measurements do not estimate model prompt-cache hits,
generation TPS, end-to-end latency or production throughput.

For one item, additional measured peak memory stayed at 2,680 bytes across 64,
512 and 4,096 text chunks. Item/part metadata and distinct unindexed reasoning IDs
grow with their cardinality, not delta text. Sparse frames additionally retain
up to the stated wire cap plus parsed/carrier representation. A response cleanup
owner retains small handles (104-byte shallow owner, 176-byte completed task and
64-byte saved-reference tuple in the measured build) until the response expires.
API teardown creates two joined cleanup tasks; interrupted classic iteration may
add one joined core-close task. Normal completed core iteration adds no task and
measured about 1.4 microseconds/request above its prior path. No new long-lived
credential cache, payload copy or unbounded background drain is introduced.
