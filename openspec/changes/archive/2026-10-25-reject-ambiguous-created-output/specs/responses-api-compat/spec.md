## ADDED Requirements

### Requirement: Several created responses cannot claim anonymous output by visibility

When a shared upstream socket has more than one pending request with a response id, a nonterminal anonymous `response.*` output event MUST remain unattributed. The matcher MUST NOT resolve this ambiguity by selecting a visible request, a draining request, or a sibling still waiting for `response.created`. HTTP bridge processing MUST deliver the event to no request queue and MUST retain its existing unmatched-liveness diagnostic. Explicit response ids, single-created-owner routing, anonymous terminal/error matching, and vendor telemetry matching MUST retain their existing behavior.

#### Scenario: One accepted response is draining

- **GIVEN** two accepted responses share a socket and one is draining after downstream cancellation
- **WHEN** an anonymous output event arrives
- **THEN** neither request is attributed that event
- **AND** the visible consumer receives no ambiguous content

#### Scenario: Both accepted responses are draining

- **GIVEN** two accepted responses share a socket and both are draining
- **WHEN** an anonymous output event arrives
- **THEN** neither draining request receives event accounting or archive ownership

#### Scenario: A pre-created sibling waits behind two accepted responses

- **GIVEN** two accepted responses and one request awaiting its own creation share a socket
- **WHEN** an anonymous output event arrives
- **THEN** the pre-created request receives no content and no request is attributed the event
