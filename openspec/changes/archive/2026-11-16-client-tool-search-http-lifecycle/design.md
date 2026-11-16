## Intent and evidence

Keep Claude Code's complete client tool-discovery histories on the existing bounded HTTP operation path. Bifrost logs store pre-provider function results containing `tool_reference`; the actual installed binary converts these to client tool-search items. A synthetic pinned-binary test asserts the HTTP wire shape independently of those logs, then demonstrates that opaque-prefix transport recovery fails.

Cache measurements are diagnostic, not proof of causality: equal one-hour windows had 92.37% and 87.54% weighted cached input; one continuing session decreased and another increased; the most recent fifteen-minute sample recovered to 99.17%. Preserve the wire cache key and all input/tool definitions verbatim. Do not change cache-key construction, pool width, account limits, protocol or pricing.

## Approach

Extend the existing complete-input proof with explicitly paired `execution=client` tool-search calls and outputs. Validate returned tool definitions as client functions. Use one client-tools predicate at operation creation; rename the internal function-only capability to client-tools-only so the controller's name states its actual contract. Keep one controller, publication gate, original deadline and replay ledger.

Alternatives considered: forcing HTTP contradicts the user's WebSocket preference; disabling tool search changes the client model/context contract; removing retirement/continuity fences risks duplicate dispatch. None is selected. This narrow compatibility choice is an implementation decision under the existing architecture, not a claim of separately approved hosted-tool support.

## Boundaries and verification

Accept complete client search pairs only; reject hosted/missing execution, absent/duplicate/cross-kind identifiers, unresolved calls, malformed arguments/loaded definitions and unknown input types. Preserve native ciphertext/status/cache fields and original request JSON. Retain existing unsafe replay, account/API-scope, file-owner, cancellation, deadline and repeat-retirement controls. Prove real route behavior for retired predecessors and the pinned Bifrost ingress; use isolated synthetic databases and upstreams only. Prepare a separate source-pinned runtime after verification; activation remains an operator action and retains the live database.
