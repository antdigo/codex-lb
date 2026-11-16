## ADDED Requirements

### Requirement: Exact response context transformations require response-owned metadata
The proxy SHALL use input-prefix and pending-obligation metadata only for the exact response and owner to which it belongs. Historical aliases SHALL retain owner routing without granting authority from a newer lane snapshot. Completed-response advancement SHALL clear unprovable prefix metadata and associate pending obligations with that same response.

#### Scenario: Older response follows independent work on the same lane
- **WHEN** an independent request advances a reused lane and a later request names an older response while supplying new history and loaded tool definitions
- **THEN** the proxy preserves supplied meaningful input and does not trim or grant anchor-free replay using the newer response snapshot

#### Scenario: A response has unproven input-prefix metadata
- **WHEN** the completed response advances but its input count or fingerprint is unproven
- **THEN** prior prefix metadata cannot become associated with the new response

#### Scenario: Marked output prefix lacks exact output evidence
- **WHEN** an anchored request supplies marked assistant or tool-call items before a tool result
- **THEN** the proxy removes that prefix only when exact owned completed-output evidence covers its meaningful content in order, preserving changed arguments, unknown metadata and unproven historical output

### Requirement: Logical HTTP operations own effective cache identity policy
The proxy SHALL resolve API-key cache identity override over overlaid fleet settings once per logical HTTP operation and retain that decision across attempts, raw handoff and owner forwarding. Account-specific cache identity SHALL be derived only after account selection and applied exactly once to canonical body and accepted process/thread handshake headers. Connection reuse SHALL respect its actual handshake policy while preserving exact continuation ownership and existing bounded lifecycle rules.

#### Scenario: Per-key policy differs from fleet policy
- **WHEN** the HTTP bridge handles a key whose shared/isolated override differs from fleet policy
- **THEN** body and upstream handshake both obey the key's effective policy consistently with raw HTTP transport

#### Scenario: Policy changes while an operation is recovering
- **WHEN** current fleet or key policy changes after a logical operation starts
- **THEN** its authorized retry retains its original decision and canonical input

#### Scenario: Later traffic encounters a connection with older policy
- **WHEN** a new operation has a different effective mode from a warm connection
- **THEN** independent work cannot silently use incompatible handshake state and an exact continuation cannot silently move to another owner or lose supplied context
