## ADDED Requirements

### Requirement: Recovery preserves meaningful tool capabilities
The proxy SHALL preserve validated portable client tool-discovery history and loaded definitions during account-neutral recovery. Hosted, unknown or account-owned discovery that cannot be proven portable SHALL prevent that recovery rather than be silently omitted.

#### Scenario: A discovered function exists only in historical discovery output
- **WHEN** verified full-context recovery includes a completed portable client discovery pair defining a function absent from the current top-level catalog
- **THEN** the replacement receives that definition and its discovery history in the original order

#### Scenario: Discovery portability is unproven
- **WHEN** discovery execution, loaded definitions or account ownership cannot be proven portable
- **THEN** recovery refuses before replacement dispatch and retains the original history

### Requirement: Independent pool work cannot inherit lane conversation authority
An independent full-context HTTP operation SHALL remain independent through final preparation even when its reused physical lane carries Codex session provenance or previously served an explicit continuation.

#### Scenario: Sibling full contexts share a warm broad-session lane
- **WHEN** a request without a specific continuation anchor reuses a compatible warm lane under a broad session header
- **THEN** its full native history and current tool catalog are sent without injecting a previous-response anchor from another operation

### Requirement: Durable completeness covers every client obligation
A durable pending-tool manifest SHALL account for every supported client call, compare observed streamed calls against terminal output, reject duplicate or cross-family identities, and remain unknown for unobserved terminal-only calls or unsupported client obligations. Legacy or unknown completeness-envelope versions SHALL NOT grant completeness authority.

#### Scenario: A parallel client search is missing from the resend
- **WHEN** a completed response requires a function result and a client tool-search result but the resend provides only the function result
- **THEN** the resend cannot obtain exact-call completeness authority

#### Scenario: A surviving legacy manifest omitted parallel discovery
- **WHEN** persisted pending-call JSON lacks the current completeness version
- **THEN** it is treated as unknown without rewriting the database or granting exact-call recovery authority

#### Scenario: Anchored work continues after an interrupted client discovery
- **WHEN** an explicit client discovery output is missing from a verified anchored follow-up
- **THEN** the existing interruption handler closes that call with a native empty client discovery result without inventing loaded definitions

### Requirement: Retry identity derives from canonical prepared input
Every physical attempt SHALL distinguish canonical prepared request data from its final account-transformed wire frame. Recovery SHALL construct the next wire frame from canonical prepared data and apply identity exactly once, preserving existing dispatch, publication, owner and deadline fences.

#### Scenario: Same-account isolated replay follows an opaque disconnect
- **WHEN** a replay is authorized before visible output on the same isolated account
- **THEN** both attempts retain the same physical cache identity and native request content

#### Scenario: A raw key resembles a scoped key
- **WHEN** the caller supplies a raw cache key resembling internal scoped output
- **THEN** identity construction follows explicit canonical provenance without suffix guessing
