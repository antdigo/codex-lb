# Responses API Compatibility Context

## Purpose and Scope

This capability implements OpenAI-compatible behavior for `POST /v1/responses`, including request validation, streaming events, non-streaming aggregation, and OpenAI-style error envelopes. The scope is limited to what the ChatGPT upstream can provide; unsupported features are explicitly rejected.

See `openspec/specs/responses-api-compat/spec.md` for normative requirements.

## Rationale and Decisions

- **Responses as canonical wire format:** Internally we treat Responses as the source of truth to avoid divergent streaming semantics.
- **Strict validation:** Required fields and mutually exclusive fields are enforced up front to match official client expectations.
- **Cursor alias compatibility:** Cursor UI model labels may append reasoning or speed suffixes to GPT-5 slugs; those are normalized to canonical upstream fields before forwarding.
- **No truncation support:** Requests that include `truncation` are rejected because upstream does not support it.
- **Compact as a separate contract:** Standalone compact is treated as a canonical opaque context-window contract, not as a variant of buffered normal `/responses`.

## Constraints

- Public-contract SSE filtering uses the `response.*` and `error` families, so
  diagnostics such as `responsesapi.websocket_timing` cannot interrupt strict
  client event deserializers. For example, a timing diagnostic between a text
  delta and `response.completed` is removed while both standard events remain.
  Native Codex requests retain vendor events; OpenAI-shaped backend requests
  follow public filtering. This does not normalize string-valued
  `response.instructions` or establish full IntelliJ compatibility (Refs #1934).

- Upstream limitations determine available modalities, tool output, and overflow handling.
- `store=true` is rejected; responses are not persisted.
- `include` values must be on the documented allowlist.
- `truncation` is rejected.
- `previous_response_id` is forwarded when `conversation` is absent, but the `conversation + previous_response_id` conflict remains rejected.
- HTTP `/v1/responses` and HTTP `/backend-api/codex/responses` now use a server-side upstream websocket session bridge by default so repeated compatible requests can keep upstream response/session continuity without forcing clients onto the public websocket route.
- Codex-affinity HTTP bridge sessions can optionally use a conservative first-request prewarm (`generate=false`), but that behavior stays behind an explicit switch so production defaults do not pay an extra upstream request unless operators opt in. The switch is the dashboard setting `http_responses_session_bridge_codex_prewarm_enabled` (Settings → Advanced → Session bridge), resolved from the settings-cache snapshot before a session's prewarm lock; its `CODEX_LB_*` environment variable is a deprecated alias that applies only while the dashboard value is unset.
- When operators configure a multi-instance bridge ring, deterministic owner enforcement now applies only to hard continuity keys such as `x-codex-turn-state` and explicit session headers. Prompt-cache-derived bridge keys remain stable for local reuse, but in gateway-safe mode a non-owner replica may tolerate that locality miss and create or reuse a local session instead of failing with `bridge_instance_mismatch`.
- Codex-facing websocket routes now advertise `x-codex-turn-state` during websocket accept and honor client-provided turn-state on reconnect so routing can stay sticky at turn granularity even when the public websocket reconnects.
- HTTP responses routes now also return `x-codex-turn-state` headers so clients that persist response headers can promote later HTTP requests from prompt-cache affinity to stronger Codex-session continuity.
- `/v1/responses/compact` keeps a final-JSON contract and preserves the raw upstream `/codex/responses/compact` payload shape as the canonical next context window instead of rewriting it through buffered `/codex/responses` streaming.
- Compact transport failures fail closed with respect to semantics: no surrogate `/codex/responses` fallback and no local compact-window reconstruction.
- Compact transport may use bounded same-contract retries only for safe pre-body transport failures and `401 -> refresh -> retry`.
- `/v1/responses/compact` is supported only when the upstream implements it.
- `prompt_cache_key` affinity on OpenAI-style routes is intentionally bounded by a dashboard-managed freshness window, unlike durable backend `session_id` or dashboard sticky-thread routing.
- Codex-native direct websocket `/backend-api/codex/responses` treats upstream `previous_response_id` as an ephemeral anchor. If that anchor goes stale, the proxy masks raw upstream details and emits the sanitized canonical `previous_response_not_found` classifier so compatible Codex clients can retry with full local history and no `previous_response_id`. The upstream Codex socket has emitted this condition both with the canonical code and as a parameterless `invalid_request_error` carrying ``Invalid `previous_response_id`.``; both shapes use the same recovery policy.
- Upstream Responses WebSockets use transport ping/pong control frames to detect a black-holed connection without confusing valid application-event silence with an idle turn. Direct and routed connections reuse `proxy_downstream_websocket_idle_timeout_seconds` for this zero-config liveness budget.
- A post-send liveness timeout is delivery-ambiguous. It remains account-neutral, is never transparently replayed, and retires the affected upstream socket so a client retry opens a fresh route without risking duplicated model work or tool side effects.
- An HTTP SSE first-event `stream_idle_timeout` is also account-neutral for health writes. The request may still exclude that account and fail over, but idle silence must not increment `error_count` or move the account into probe/drain.
- HTTP bridge settlement ownership is explicit: `closed` rejects new work but does not imply that a submitter owns existing siblings. Only a liveness-failed send claims whole-deque settlement under the lifecycle lock; otherwise the reader remains responsible for settling pending requests when the transport dies.
- A DRAINING durable row with a live lease is still owned. Foreign `claim_live_session` and local session create must not steal it, including when forced recovery would otherwise run because the owner endpoint is missing; expired or ownerless DRAINING rows remain recoverable.
- Hard-affinity retry-circuit evidence is request-lifecycle evidence: retirement counts only while the bridge still owns an eventless pending request. Idle no-pending retirement remains observable but neutral, so routine socket churn cannot manufacture the first strike for a later real timeout.

## Fast Mode and Service Tiers

codex-lb accepts the OpenAI/Codex `service_tier` field on Responses and Chat
Completions compatible routes. The legacy `fast` spelling is accepted as an
alias and is forwarded upstream as the canonical `priority` tier.

Fast Mode is request-level intent, not a local speed guarantee. The upstream
Codex backend decides the actual tier for each completed response. codex-lb
therefore records three separate values in request logs:

- `requestedServiceTier`: what the client or API key asked for, after alias
  normalization.
- `actualServiceTier`: what upstream reported in the completed response, when
  upstream included it.
- `serviceTier`: the effective billable tier. This uses `actualServiceTier`
  when present and falls back to `requestedServiceTier` only when upstream omits
  the actual tier.

If a request is sent with `service_tier: "fast"` or `service_tier: "priority"`
and the completed row shows `requestedServiceTier: "priority"` but
`actualServiceTier: "default"`, codex-lb forwarded the priority request and
upstream chose the default tier. That can happen even when websocket transport
is active.

For OpenCode or Codex-compatible clients, enable Fast Mode by sending a
Responses request with:

```json
{
  "service_tier": "priority"
}
```

Clients that expose Fast Mode as `fast` may keep using that spelling; codex-lb
normalizes it to `priority` before forwarding.

### Ultrafast Processing

The [OpenAI Responses API reference](https://developers.openai.com/api/reference/resources/responses/methods/create)
documents `ultrafast` as an access-controlled processing tier currently
available for `gpt-5.6-sol`. codex-lb forwards this canonical value unchanged;
it does not grant Ultrafast access by itself.

Account eligibility comes from live or retained per-account upstream catalog
metadata. The bundled bootstrap catalog deliberately does not advertise
Ultrafast. If no account advertises the tier, an explicit Ultrafast request
cannot select an eligible account; API-key enforcement follows the existing
model-capability fallback when the model itself does not advertise the tier.

Send a Responses request with:

```json
{
  "model": "gpt-5.6-sol",
  "input": "Summarize the change.",
  "service_tier": "ultrafast"
}
```

After completion, verify that the response reports
`service_tier: "ultrafast"`. Request logs retain `ultrafast` in the requested,
actual, and effective billable tier fields when upstream confirms it.

### Operator Fast Mode prohibition

Operators can enable the Routing setting `prohibitFastMode` when qualified
Codex harness model aliases such as `gpt-5.6-sol-xhigh-fast` must run at the
normal OpenAI tier. The alias still supplies its canonical model and reasoning
effort, but does not derive `service_tier: "priority"`. This policy does not
rewrite an explicit client tier or an API-key-enforced tier; see
`openspec/specs/fast-mode-policy/context.md` for scope and operating notes.

API keys can also force the tier for traffic that uses that key. Set the key's
enforced service tier to `priority` or `fast`; both values are stored and
returned as `priority`.

To verify a completed Fast Mode request:

1. `Transport` should be `WS` if you are verifying the websocket Codex path.
2. `requestedServiceTier` should be `priority` when the client requested Fast
   Mode or the API key enforced it.
3. `actualServiceTier` is the upstream result. `default` means upstream did not
   grant priority for that response.

This distinction matters for quota and cost accounting: codex-lb prices the
request from the effective billable `serviceTier`, not from the requested tier
when upstream reports a different actual tier.

## Include Allowlist (Reference)

- `code_interpreter_call.outputs`
- `computer_call_output.output.image_url`
- `file_search_call.results`
- `message.input_image.image_url`
- `message.output_text.logprobs`
- `reasoning.encrypted_content`
- `web_search_call.action.sources`

## Failure Modes

- **Stream ends without terminal event:** Emit `response.failed` with `stream_incomplete`.
- **Upstream error / no accounts:** Non-streaming responses return an OpenAI error envelope with 5xx status.
- **Compact upstream transport/client failure:** Retry only inside `/codex/responses/compact` when the failure is safely retryable; otherwise return an explicit upstream error without surrogate fallback.
- **HTTP bridge session closes or expires:** The next compatible HTTP `/v1/responses` or `/backend-api/codex/responses` request recreates a fresh upstream websocket bridge session; continuity is guaranteed only within the lifetime of one active bridged session.
- **Multi-instance routing without bridge owner policy:** if operators do not configure a bridge ring or front-door affinity, continuity can still fragment across replicas. With a configured bridge ring, hard continuity keys landing on a non-owner replica are proxy-forwarded to the owner replica; the proxy fails closed only when the owner endpoint or ring membership cannot be resolved or the forward signature fails authentication. Gateway-safe prompt-cache requests may accept locality misses and continue locally instead of forwarding.
- **Codex websocket reconnects:** Reconnect continuity now depends on the client replaying the accepted `x-codex-turn-state`; generated turn-state is emitted on accept for backend Codex routes and echoed back when the client already supplies one.
- **Codex websocket stale previous-response anchors:** Direct backend Codex websocket stale-anchor failures are either replayed transparently from a self-contained full resend or surfaced as a sanitized `response.failed` whose `response.error.code` is `previous_response_not_found`; the error omits `param`, the raw upstream envelope, and the missing `resp_...` id. A connect-time failure uses the same code directly at `error.code`. This includes the parameterless upstream message ``Invalid `previous_response_id`.``. OpenAI-compatible `/v1/responses` websocket clients continue to receive generic `stream_incomplete` masking.
- **Websocket handshake forbidden/not-found:** Auto transport now fails loud on `403` / `404` instead of silently hiding the websocket regression behind HTTP fallback.
- **Upstream websocket stops answering pings:** Pending direct-WebSocket and HTTP-bridge work fails with `upstream_websocket_liveness_timeout`; the account remains healthy and the request is not replayed because upstream acceptance is unknown.
- **Repeated eventless bridge failures:** Two consecutive request-affecting pre-response failures can open the hard-key cooldown. A successful terminal response clears the state; an idle close followed by one real timeout remains only one strike.
- **Invalid request payloads:** Return 4xx with `invalid_request_error`.

## Error Envelope Mapping (Reference)

- 401 → `invalid_api_key`
- 403 → `insufficient_permissions`
- 404 → `not_found`
- 429 → `rate_limit_exceeded`
- 5xx → `server_error`

## Examples

Non-streaming request/response:

```json
// request
{ "model": "gpt-5.1", "input": "hi" }
```

```json
// response
{ "id": "resp_123", "object": "response", "status": "completed", "output": [] }
```

Cursor-style model alias request:

```json
{ "model": "gpt-5.4-mini-high", "input": "hi" }
```

This forwards upstream as `model: "gpt-5.4-mini"` with `reasoning.effort: "high"`.

Retry-circuit accounting example: an idle bridge closes with `pending=0`, then
the next request times out before `response.created`. The idle close is logged
but contributes no failure; the timeout is the first strike. Only another
consecutive eventless pending failure may open the repeated-failure cooldown.

Stale-anchor recovery example: a reconnect sends a tool-output delta with a
recent `previous_response_id`, and upstream answers
``{"type":"error","status":400,"error":{"type":"invalid_request_error","message":"Invalid `previous_response_id`."}}``.
Because the delta cannot stand alone, codex-lb returns a sanitized
`previous_response_not_found` signal on the Codex-native route so the client can
retry once with full local history. If the original request already contained a
self-contained full resend, codex-lb instead reconnects and replays that body
without the rejected anchor.

## Previous-response replay owner fencing

Removing a stale continuation anchor does not make every retained body
portable. Encrypted reasoning, account-scoped items, file references, and
durable bridge operation identities remain owned by the account that first
received them. The proxy records that dispatch owner and requires it on later
HTTP streaming, HTTP bridge, and direct WebSocket selections.

For example, if account A first receives encrypted reasoning and then returns a
pre-visible Trusted Access or authentication failure, account B must never
receive the retained ciphertext. One forced token refresh may replay the body
on account A; permanent failure or owner unavailability fails closed.

Verified recovery installs a replacement body and updates owner state
atomically. A canonical account-neutral replacement clears the owner and may
use normal failover. A verified nonneutral replacement, including a
Responses-Lite full resend, may replay only on the same owner and preserves the
fence.

HTTP bridge tracing archive IDs do not pin neutral requests. A real durable
`operation_id` does pin the request until an explicit operation-rebind path
replaces that identity. Existing file pins and API-key settlement-before-health
ordering remain independent invariants.

Streaming selection authorizes owner compatibility before opening upstream, but
persists a new owner only after dispatch is observed. A transport failure that
is positively classified as pre-dispatch therefore leaves the body unowned and
eligible for its first real dispatch on another account. Ambiguous failures
remain owner-bound.

## Known Client Integrations (Reference)

Third-party agents that consume the `/v1` Responses surface documented by this
capability (rendered guide: `docs/client-setup.md`). These are configuration
examples against the existing contract, not separate compatibility surfaces:

- **OpenCode** — built-in `openai` provider with a `baseURL` override; uses the
  Responses API path so `encrypted_content` / multi-turn reasoning state is
  preserved (Chat Completions custom providers drop it).
- **OpenClaw** — custom provider with `"api": "openai-responses"` against
  `/v1`; Codex-native provider builds may target `/backend-api/codex` instead.
- **Hermes Agent** (Nous Research) — named custom provider with
  `api_mode: codex_responses` against `/v1`; the responses transport carries
  reasoning state across turns like the OpenCode path.

New client guides added to `docs/client-setup.md` should stay configuration-only
examples of this contract; anything needing new proxy behavior requires its own
OpenSpec change first.

## Operational Notes

- Pre-release: run unit/integration tests and optional OpenAI client compatibility tests.
- Smoke tests: stream a response, validate non-stream responses, and verify error envelopes.
- Post-deploy: monitor `no_accounts`, `upstream_unavailable`, compact retry attempts, and compact failure phases, especially on direct compact requests.
- Post-deploy: monitor HTTP bridge reuse/create/evict/reconnect counts and any `previous_response_not_found` or queue-saturation errors on `/v1/responses` and `/backend-api/codex/responses`.
- Post-deploy: monitor `capacity_exhausted_active_sessions`, Codex-session bridge reuse/evict counts, websocket handshake 403/404 rates after the narrower auto-fallback policy, and backend Codex HTTP vs websocket cache-ratio gaps.
- When tracing compact incidents, confirm that request logs and upstream logs show direct `/codex/responses/compact` usage without surrogate `/codex/responses` fallback.
- Post-deploy: monitor `no_accounts`, `stream_incomplete`, and `upstream_unavailable`.
- Post-deploy: monitor `upstream_websocket_liveness_timeout`; recurring failures indicate a host route, VPN, proxy, or intermediary that black-holes established WebSockets.
- Post-deploy: correlate retry-circuit `opened`, `half_open`, and `reset` events with bridge `pending` and `response_events_seen` diagnostics. An idle `pending=0` retirement must not precede an immediate two-failure cooldown.
- Post-deploy: monitor `previous_response_not_found` on `/backend-api/codex/responses`; recurring spikes show repeated continuity failures, which may come from malformed client identifiers, server-side invalidation, or connection lifecycle. Clients should perform the documented full-context retry without `previous_response_id`. Investigate socket-lifecycle remediation only when a separate close-reason, reconnect, or transport diagnostic correlates with the failures.
- Websocket/Codex CLI tier verification runbook: `openspec/specs/responses-api-compat/ops.md`


## HTTP continuation promotion

Healthy native HTTP requests use normal policy. The proxy cannot infer every
client-local WebSocket failure from HTTP alone; it uses its existing 60-second
upstream-connect failure marker as concrete failure evidence. Operator HTTP
pins and size bypasses remain effective. The image bypass keeps requests off the
HTTP session bridge but no longer pins the upstream transport, which is resolved
by ordinary precedence; an `input_image` request keeps upstream HTTP only when
its payload exceeds the WebSocket frame budget or still carries an external
image URL. External-URL detection for that decision recurses the whole input, so
a URL nested inside a tool-output array keeps the pin even though the image
inliner never rewrites it — that is the case where the URL is still external at
the upstream. The inliner and the bridge's post-inline guard still read only
top-level `input_image` items and one level of `content`; closing that is a
separate change. No new retry/session registry.

History-only locality is soft, scoped by the bridge's full API-key identifier,
and hashes the complete first user item plus instructions and model. No client
prompt cache field is overwritten. Identical initial prompts may share an idle
connection, but neither histories nor response anchors are merged; the complete
request is sent each time. Existing hard-continuity paths retain their guarded
incremental replay. Conversation IDs get their own hashed locality and are never
combined with an injected previous_response_id.

Chat keeps the existing stream conversion/usage/error/cleanup pipeline and uses
the bridge only after the source-routing branch. Backend stream=false retains
its native non-streaming upstream contract. No claimed latency percentage:
connection reuse is measured separately from admission and successful transport.

For example, a Chat client sending `[user(task), assistant(answer), user(next)]`
without session headers can open a bridge connection. Appending the next
assistant/user pair reuses that connection while sending the entire new history.
The same initial task under a different API key selects a separate connection.

Chat binds existing settlement ownership signals while advancing its bridged
stream. Predispatch failures and cancellation release origin-owned reservations;
accepted or delivery-ambiguous owner forwards retain their settlement owner.
Context bindings do not span yields because startup probes and consumers may
advance the stream from different tasks.

## HTTP response ownership before delivery

An HTTP response can expose its upstream ID before the detached request-log write finishes. The stream now publishes each authoritative lifecycle ID to the existing bounded process cache before delivering that event, beginning with `response.created` when present. An immediate follow-up can resolve the selected account while the original stream or its log write is still pending. This is same-process owner readiness; it does not promise that an unfinished response is already usable by the upstream provider.

The cache retains its existing API-key partition, session-first lookup and same-key fallback. Durable lookup and unknown-owner rejection remain the miss path. Publication adds no synchronous persistence barrier, registry or cross-replica readiness guarantee, and does not strengthen session identifiers into a new authorization boundary.

Local failure events and locally assigned response IDs are separate facts. `ParsedSseBlock.is_local` identifies generated events; `response_id_is_local` excludes a generated ID even when SDK normalization wraps a real upstream error. These flags remain outside serialized event bytes and survive parsed-payload reattachment. Thus an oversized-frame failure cannot invent an upstream owner, while a real upstream error with a locally assigned ID remains a valid event for timing. The shared HTTP/direct/routed WebSocket normalizer and this provenance contract are owned here; HTTP timing consumes them through the owner dependency. Existing durable-log behavior is unchanged. See the [ownership requirement](spec.md#requirement-observed-http-response-ids-publish-same-process-ownership-before-delivery).

Canonical background JSON acknowledgements with status `queued` or `in_progress` carry the same authoritative response identity as SSE lifecycle events. The lifecycle parser includes both, and the HTTP relay keeps queued events on the parsed path. For example, a two-account request receiving a queued acknowledgement can immediately route a same-process continuation to its known account while its originating log is pending. An in-progress event following token output has the same ownership behavior. The provider still decides whether unfinished work can be continued.

## Detached retirement sweep deadline

Issue #2149 bounds aggregate detached-session lock waiting during request finalization. A sweep shares five seconds: if its first attempt consumes three seconds, the next receives two, and later attempts stop at expiry. Deferred generations remain tracked for later requests and their lifecycle owners. The deadline does not cancel resource-close owners or replace their existing close timeout.

## Routed file transport failover

File operations follow the existing pre-visible unary retry policy. The file client carries typed dispatch provenance through its service adapter so a refused account-proxy connection can retry another eligible account even though credential-safe transport messages omit low-level details. Typed replay eligibility takes precedence over message text: a proxy endpoint named `timeout-primary` must not turn a TLS verification failure into a retry.

For example, an unpinned upload whose account A cannot connect to its proxy can complete through account B, and the resulting file owner pin belongs to B. Finalization of a file pinned to A remains on A. Ambiguous request delivery, body-read failures, and host-wide network failures do not permit cross-account retries.

Finalization may issue several upstream polls within one downstream call. A refused connection on its first poll can still use an eligible fallback account. After a poll has returned `retry` from A, however, a later refused connection must fail the operation on A: the pre-dispatch status of the later request does not undo the earlier poll's account-local progress. This applies even when no file owner pin was found; direct transport polling retains its existing behavior.

## Shared bridge response ownership and terminal recovery

A pending response belongs to the physical socket on which it was submitted. Reconnecting one retryable turn cannot carry an accepted or draining sibling onto the replacement socket. The early retry guard preserves replay state; the common reconnect entry checks pending ownership before account selection. Prewarm timeout identifies only its own disposable probe and holds the lifecycle lock while reconnecting, so a submitter already past its closed-session check finishes publication before recovery evaluates ownership. Other reconnect callers retain their existing lifecycle or response-create gate fencing.

Shared prompt-cache locality cannot identify the owner of an anonymous output frame. When several responses are already created, downstream visibility or cancellation does not disambiguate them. Such a frame retains the unmatched-liveness diagnostic and no request queue or archive owner. Single-created-owner routing, explicit response ids, anonymous errors/terminals and vendor telemetry keep their existing rules.

For example, response A is accepted while B awaits creation on the same socket. If that socket fails, replaying B alone would strand A's response id. Recovery instead declines and the existing reader funnel settles both. Separately, a public response that has emitted creation and metadata but no output can recover complete item/content/tool-argument events from its own authoritative terminal envelope. Recovery requires the same response id, emits no second creation, and assigns increasing generated sequence numbers. Partial streamed output is not replayed by this recovery. Public data after the first terminal is suppressed so duplicate completion or late argument frames cannot duplicate a tool lifecycle. Native passthrough retains its existing normalization behavior.

These changes add no growing buffer or long-lived cache. Reconnect ownership scans run only during recovery; the multiple-created-owner check adds a length comparison to the list the matcher already builds. Public normalization retains one response-id reference and one boolean per stream. A 14-event ordinary-stream benchmark on the implementation host measured 1,066,705 to 1,075,806 CPU ns/stream in a repeat run, with peak traced bytes 13,627 to 13,651; a loaded run measured a 5.4% CPU increase. These are local Python normalizer measurements, not an end-to-end latency guarantee or an allocation-count measurement. No configuration, database schema or native helper protocol change is involved.

## Retirement during canonical registry reuse refresh

The canonical session-key registry reuse branch checks retirement again after its awaited durable lease refresh. An older response may detach during that await without acquiring the registry lock. A newly retiring candidate without a reserved unanchored handoff proceeds through ordinary drain-aware replacement before submission: visible siblings retain their old socket, pending ownership and settlement; client anchors and file/account pins retain their existing owner constraints. A reserved unanchored handoff retains its existing generation authority.

This closes the reproduced refresh window, not all retiring rejections. Retirement after lookup still reaches the pre-dispatch fail-closed fence, and the separate previous-response-alias refresh branch is outside this change. No upstream frame is replayed by the new branch. Limited-key regression cases prove one finalized/released reservation, and the positive unanchored case proves one physical send with zero replay count.

The common path adds a retirement flag check and boolean return with no retained buffers/cache. An isolated helper benchmark with I/O/logging stubbed measured 1,273.96 to 1,382.48 ns CPU per call (+108.52 ns, +8.52%; 11 interleaved samples of 100,000 awaits). These figures are not end-to-end gateway latency. There are no configuration, database schema or native protocol changes and no native rebuild requirement.

## Guarded idle prompt-cache socket lifetime

A canonical soft prompt-cache bridge can replace a completely unowned physical socket at 3000 seconds before sending a new unanchored HTTP request. Physical opening time is distinct from idle last-use time and resets on each successful connection. The scope excludes Codex continuity sessions, original/effective response or conversation anchors, turn/session headers, forwarded requests, Responses-Lite and recovery keys.

For this scope, the original Responses input defines the entire new context; a soft prompt-cache key is a cache hint, not implicit conversation state. The narrow guard accepts complete inline messages, complete function calls and outputs, and reasoning with an ID plus either encrypted content or full explicit reasoning_text parts. Function output can be a string or a nonempty array of complete input_text parts. Function-call IDs must be uniquely paired with subsequent full outputs. Unknown/reference-only shapes and opaque encrypted function arguments remain ineligible. A full cancelled function output may retain native `status=incomplete`. The policy constructs no projection and sends the canonical prepared input, ciphertext, call IDs and PCK unchanged on the same account.

Native text carriers are inspected without rewriting them. For example, a Claude Code history containing both encrypted reasoning and explicit reasoning_text can rotate an unowned aged socket before sending the next turn. A cancelled function output containing input_text blocks keeps its array and status. Missing text, summary-only reasoning, malformed supplied ciphertext and unknown content parts do not gain rotation eligibility. This addresses a conservative false negative; busy or anchored generations and accepted response recovery retain their existing policies.

The old account becomes a request-scoped mandatory owner. The central reconnect boundary enforces it even if an ordinary soft preference is cleared; higher-level reroutes that cannot retain the constraint fail closed before another send. A replacement handshake failure cannot escape through an unpinned raw HTTP fallback. The original request deadline and reservation remain in use.

Age retirement requires an empty literal pending deque, zero queued/admission owners, no handoff/reservation/close owner or reconnect retirement, and no reader still processing a frame. The reader marker includes terminal persistence after a request has been popped. Trylock uncertainty skips rotation. Detachment immediately assigns a cancellation-safe cleanup owner and also enters the existing planned synchronous-close list, so a failed owner lookup cannot strand the socket and a one-session cap can wait for its own predecessor rather than evicting an unrelated session. Both close paths share one resource-close task.

For example, a 3593.4-second-old idle socket receives no second create when the next complete unanchored request arrives: its same-account replacement receives that request once. This does not cover busy bridges, raw downstream WebSockets, late-submit races or transparent replay after accepted model output. The roughly 600-second headroom before the observed one-hour limit is supported by a sample of successful streams, not a bound on future generation duration. Prompt-cache hits after replacement remain upstream-dependent.

Retained state is two small session scalars and one optional account-ID reference per request state. Shape checking uses transient call-ID sets bounded by that input's function-call count. The young-socket path adds a registry/age hint and later owner-marker checks, without an age capability, body traversal, history copy, new database query or long-lived cache. An isolated hint microbenchmark measured about 616 ns/op above an empty-function baseline on the implementation host; this is not an end-to-end latency or allocation-count measurement. There are no configuration, database schema, dependency or native protocol changes.


## HTTP bridge execution ownership

Each HTTP bridge serializes upstream execution independently of response-created admission. A subsequent same-socket request waits locally before response.create under the original admission/request budget; its cancellation or wait timeout cannot abort the already executing response. For example, a long tool-argument response A may complete while waiter B returns response_execution_wait_timeout, and C can reuse the warm socket. Account-create admission still releases on response.created. The independent execution handle releases on terminal or fenced cleanup, including prewarm and reader failure. Separate physical sockets keep concurrency; no upstream stream_id capability is assumed.

## Execution admission and stream ownership

[HTTP bridge execution and create admission](http-bridge-admission.md) explains
the two wait budgets, retained deadlines, account binding and queue lifetime.
[Stream ownership, lifecycle and cleanup](stream-ownership.md) covers response
identity, SDK recovery, bounded ordering, teardown and measured processing cost.
The normative requirements remain in [spec.md](spec.md).

## Pre-response connection lifetime recovery

The HTTP bridge already rotates eligible idle prompt-cache sockets before the
upstream lifetime limit. Busy or owned sockets can outlive that rotation window.
An explicit connection-limit rejection before response acceptance supplies a
bounded recovery opportunity without changing the request's account owner.

For example, Claude Code can submit its full reasoning and tool history through
Bifrost on an existing prompt-cache socket. If that socket rejects response.create
with the explicit lifetime error, the bridge sends the same context once on a new
socket for the same account. An unavailable owner, expired deadline, detached
consumer or a second transport failure terminates the request.

This policy does not replay an accepted response after a service-restart close
(1012). Such a turn may already have generated output or performed tool work.
Raw downstream WebSocket recovery keeps its existing policy.

## Retirement of an execution waiter

A soft prompt-cache request can still be waiting for execution when an older
turn's socket closes or starts draining. The local execution fence rejects
that request before it sends anything. A cleaned, unanchored waiter can move
once to a bridge for the same account, keeping its prepared context, API-key
reservation and original deadline. For example, two queued Claude Code calls
can both complete after the earlier socket retires without sending either call
twice. An accepted response or a request carrying a continuity anchor does not
qualify for this rule.

The replacement uses the prepared request snapshot, including any already
inlined images. Admission and persistence can consume the remaining budget, so
dispatch checks the original deadline again after those waits. An unavailable
owner, expired budget, cancellation or a repeated retirement ends the request
through the existing settlement paths. No schema, dependencies, native helper
or Bifrost configuration change is required.

Selecting this replacement also consumes the turn's pre-dispatch reconnect
budget. A close during preparation or after admission cannot open another
physical socket before the first response.create attempt. A separate
request-scoped flag fences the common reconnect entry point without spending
the replay counter: an eligible connection-limit error after an actual send
still follows the existing bounded same-account recovery policy.

## Operation lifecycle and rollout context

A prompt cache key is a reuse hint shared by independent agents. It does not
identify a conversation. Independent complete requests use a bounded group of
seven logical lanes; an exact response or turn anchor keeps its lane and order.
Broad session/thread headers retain group locality rather than selecting a
single physical lane. Account eligibility for new independent work is checked
against the current API assignment without moving existing anchored work.
The account stream cap remains eight, with one slot reserved for the existing
recovery admission path. Physical sessions remain subject to the existing global
session limit and idle eviction policy.

For example, seven agents can submit complete conversations with the same cache
key concurrently. A follow-up containing a previous response ID waits for that
response's lane, while an independent eighth request can use whichever compatible
ordinary lane next becomes available. A lane queue survives replacement of its
physical WebSocket. Waiting references pin the required generation against idle
eviction; these pins do not acquire account stream leases.

The publication boundary separates replaceable work from output already exposed
to the client. Readable reasoning is output. Only an understood opaque reasoning
prefix can remain unpublished, bounded to 256 KiB. An allowed replacement keeps
the public response ID and translates an anchored continuation to the winning
physical response ID. Item IDs and ciphertext are retained unchanged. A lost
process-local binding fails closed, using an existing owner-fenced durable alias
marker to distinguish projected IDs. No process-restart recovery or database
migration is introduced.

Dispatch authorizations are single-use and checked at the physical send boundary.
Close, timeout, cancellation and terminal observations produce controller-owned
decisions; existing socket, lock, lease and persistence code executes those
decisions. Resumable resource receipts preserve compensation after an interrupted
terminal write. The original absolute deadline spans pool waits, re-preparation
and a proven-unsent local transport handoff.

An abandoned physical attempt may have consumed tokens without returning usage.
Its usage is marked unknown; reported final usage belongs to the winning attempt.
The system cannot infer an unreported upstream charge.

Specific continuations retain the existing verified full-resend recovery path
even when their physical owner originated in the independent pool. For example,
an exact response/turn follow-up whose owner explicitly rejects the old response
may replay its verified complete context under the existing durable operation
fence. Moving that particular turn alias requires a captured source-owner/alias
proof and reversible registration; a private pool key is not itself permission
to move aliases. Changed ownership or a concurrently moved alias fails closed.

### Validation and deployment boundary

The source change uses synthetic upstream sockets, temporary databases and an
isolated Bifrost 2.5.1 process for Anthropic conversion acceptance. Production
credentials and prompts are not acceptance fixtures. The normal test suite skips
this optional binary test unless `CODEX_LB_BIFROST_ACCEPTANCE_BINARY` is supplied.

Deployment is separate from source implementation. Before activation, seal the
candidate with its base revision, complete source hashes and patch digest; retain
the previous runtime selection. Stop and back up application data using the
existing service-user SQLite backup procedure. Since this change does not alter
the database schema, rollback selects the previous runtime while retaining live
database and refreshed authentication tokens. Do not restore an old database
snapshot merely to roll back this code change.

Process-local projected continuation bindings do not survive a runtime switch.
Drain active operations and resume clients with complete unanchored context,
without the old `previous_response_id` or turn-state anchor. This is especially
necessary when selecting an older runtime that has no projection translator.
Retaining the live database preserves tokens and routing records, not the lost
process-local translation map.

The canary gate requires at least 65 minutes and 100 comparable requests. Observe
correctness, active/queued operations, session and lease cleanup, failure kinds
and comparable cache-hit ratios. Stop expansion on duplicate or misrouted output,
leaked ownership/leases, or a cache-hit ratio drop greater than five percentage
points. Synthetic acceptance is evidence of conversion behavior, not evidence
that this operational canary has run.

## HTTP transport boundary corrections

An explicit raw HTTP refusal after a proven-unsent bridge handoff must acquire a controller grant before a retry. The refusal uses the existing raw failure classification and consumes the same ordinary replay quota as transport/capacity/continuity recovery. The original deadline, publication gate and account/file restrictions survive the handoff.

A public response ID retained across opaque replay is not a portable physical anchor. When raw HTTP cannot preserve its bridge binding, the request fails locally before dispatch. For example, a public `resp_original` bound to a winning replacement response must never be sent to raw upstream as though it named that replacement. Local bindings and a durable existence-only marker recognize this case without exposing another API key's owner or response. Ordinary physical response IDs retain their raw path. Marker lookup shares the remaining request budget.

## Client-executed tool discovery and HTTP operation completeness

Bifrost 2.5.1 can map Claude Code's client ToolSearch function and tool-reference results into native `tool_search_call` / `tool_search_output` pairs. The installed-binary synthetic acceptance test asserts that actual HTTP wire shape before exercising codex-lb. Treating these items as opaque excluded otherwise complete histories from the independent affinity pool and pre-publication recovery, leaving parallel agents on the legacy canonical execution queue.

The complete-input proof now recognizes explicit client-executed pairs with unique call IDs, object arguments and a matching output containing complete client function definitions. Search and ordinary function pending IDs remain separate; a function output cannot settle a search and vice versa. Hosted or unspecified search execution, unresolved/duplicate pairs, malformed loaded definitions and unknown item types remain outside this proof. The operation capability is named `client_tools_only`: functions and explicitly client-executed tool search may qualify, whereas server tool search, web search, computer execution and other hosted tools do not. Native input, loaded tools, ciphertext, status and cache keys are inspected without rewriting or reordering. Existing operation quotas, publication, account/API ownership, original deadline and settlement remain authoritative.

For example, two complete same-cache histories that contain an already answered client tool-search call can use separate compatible lanes while one physical predecessor is occupied. An eligible opaque-prefix upstream restart can use the existing same-account replay once; readable output still closes that option. This does not establish replay safety for every builtin tool or every native item kind.

Cache monitoring must compare matched workload windows and continuing sessions, rather than interpret a process restart or an HTTP 200 as cache/reliability success. In the observed rollout the matched hour dropped from 92.37% to 87.54% cached input tokens, the two continuing sessions moved in opposite directions, and a subsequent fifteen-minute window reached 99.17%. Those measurements establish changing cache outcomes, not a causal attribution to this compatibility gap. A live rotation/cache canary remains required after operator activation.

Reference: [OpenAI client-executed tool search](https://developers.openai.com/api/docs/guides/tools-tool-search). The proxy preserves the supplied native contract rather than converting discovery history itself.


## Tool capability continuity and error-induced cache misses

Input completeness, portability and durable exact-call completeness are separate policies sharing a pure tool vocabulary. Portable client discovery remains meaningful history, including loaded function definitions absent from the current catalog. Unsupported hosted, unknown or account-owned discovery refuses portability rather than disappearing. A physical lane's old Codex-session flag cannot authorize anchoring, trimming or interrupted-output injection for a newly independent request. Exact request continuations retain existing account/API/owner fences. Interrupted client discovery receives an empty native tools result, never a function-shaped string output.

Every client obligation must participate in a durable pending-call proof; unknown calls or unrepresentable loaded-definition updates make the whole call-only manifest unknown. The existing JSON envelope now uses completeness version2. Older writers could omit parallel search calls, so unversioned/unknown envelopes grant no completeness authority; no schema migration or production data rewrite is involved.

For example, an opaque 1012 retry previously retained the final account-scoped frame and applied account identity again: a short key acquired a second scope suffix, while a long key was re-digested. ResponseCreateAttempt now retains explicitly prepared pre-egress text in its existing string slot, and retries derive final egress from that snapshot. Inline-prepared input, ordered history/tools and logical key survive; account-specific identity is applied once. The final wire remains local to exact sizing/send. No new cache, retry loop, setting, prewarming or wider pool is introduced.

Stable prepared/wire input removes confirmed avoidable misses; it does not prove a provider cache hit or attribute prior aggregate hit-ratio changes. Connection-local continuation state and provider prompt caching remain distinct. Evaluate real comparable traffic for at least65minutes and100completed requests, compare weighted cached/input tokens within matched scope/model/tier/transport/context cohorts, retain unknown usage and errors separately, and investigate drops over5percentage points. No paid probes are needed to fill the sample.


### Session/cache affinity ownership correction (2026-11-16)

A response alias retains routing authority, not the latest lane snapshot's trimming or replay authority. Example: after independent B follows A on one warm lane, C anchored to A preserves supplied B-only assistant/call/discovery history. Exact physical response/account or an existing verified public→physical binding is required before using snapshot context. A bounded transient output-identity tuple verifies marked-output deduplication; absence/overflow/uncertainty preserves input. It grants neither retry permission nor historical response storage. Known individual pending calls remain distinct from a complete durable obligation manifest.

HTTP operations resolve API-key cache identity override over overlaid fleet settings once. Canonical bodies/headers stay unscoped internally; egress derives the selected account identity once for body and physical handshake. Independent requests require mode-compatible lanes. Exact continuations retain the owner and locally refuse incompatible policy rather than silently relocating. Signed owner forwarding binds the immutable policy to its existing full-context signature. New policy-bearing forwarding requires a compatible owner version; legacy no-policy signatures retain their encoding. Direct downstream WebSocket identity coverage remains the pre-existing explicit non-goal.

This removes demonstrated local causes of cache fragmentation/context loss; it does not establish provider cache residence or a hit-rate increase. No new pool width, retries, prewarm, cache controls, DB schema, dependencies or user settings are introduced. Installation remains a manually selected separate code-only runtime with rollback retaining live DB/authentication state.


### Exact-repeat advisory locality follow-up (2026-11-16)
A completed lane's stored input fingerprint can rank an equal full input as well as a strict extension. Example: concurrent independent A and B finish on two compatible idle lanes; an exact repeat of B should prefer B's lane. This is advisory locality only: account/API/model/tier/mode compatibility, hard continuation ownership and the strict trim proof still decide authority. Missing/mismatching fingerprints earn no preference. No provider cache-hit or billing improvement is implied; no new cache state, retries, prewarm or pool growth is added. The older owner-loss projection wording now preserves validated portable client discovery/loaded definitions and refuses unsafe discovery consistently with the current recovery implementation.
