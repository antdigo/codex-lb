## ADDED Requirements
### Requirement: Operation authority survives HTTP transport boundaries
The proxy SHALL preserve its original operation deadline, publication gate and shared replay quota when a proven-unsent bridge request changes to raw HTTP. A conclusive HTTP refusal recognized by existing raw retry policy SHALL obtain matching controller authority before a subsequent physical send, retaining account and file restrictions. Unknown send outcomes SHALL NOT acquire retry permission from status-code resemblance alone.
#### Scenario: Explicit generic rate limit before acceptance
- **GIVEN** a proven-unsent bridge handoff and a first raw response with status 429 and an ordinary rate-limit error before any accepted lifecycle
- **WHEN** existing raw policy retries and the original deadline and ordinary quota remain available
- **THEN** the authorized retry may complete without a dispatch-permit failure
#### Scenario: No replay authority remains
- **GIVEN** the ordinary quota is exhausted, output is committed, the deadline has expired or send outcome is ambiguous
- **WHEN** a raw retry is proposed
- **THEN** no unauthorized physical request is sent

### Requirement: Projected continuation ownership precedes transport bypass
Every HTTP routing path SHALL validate a recognized projected response anchor before any upstream dispatch. The proxy SHALL preserve its verified winning physical response and owner/lane/generation/API scope, or refuse before dispatch when the binding cannot safely be used by that route. It SHALL NOT send an abandoned public response ID as a physical anchor.
#### Scenario: Automatic fallback after successful replay
- **GIVEN** an opaque-only first attempt is replaced successfully while retaining a stable public response ID
- **WHEN** the next anchored delta meets an active upstream WebSocket failure marker
- **THEN** it uses a safely verified owning continuation or is refused before upstream dispatch
#### Scenario: Binding evidence is unavailable or stale
- **GIVEN** a projected anchor has missing, stale, wrong-generation, wrong-account, foreign-API or lost process-local binding evidence
- **WHEN** routing would bypass bridge submission
- **THEN** it fails closed without leaking that public ID to raw upstream
#### Scenario: Ordinary physical response ID
- **GIVEN** a request uses an ordinary upstream response ID without a projected-alias marker
- **WHEN** raw HTTP routing is selected
- **THEN** existing owner and retry behavior is retained
