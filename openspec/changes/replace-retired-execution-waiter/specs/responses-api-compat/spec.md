## ADDED Requirements

### Requirement: Retired HTTP execution waiter replacement

The HTTP bridge MUST replace a closed or draining bridge at most once when
`response_execution_session_retired` is a local pre-dispatch refusal for a
cleaned soft prompt-cache turn with no attempted response.create, accepted
identity, response event, visible output or downstream sequence. The turn MUST
have no previous-response, hard continuity, conversation or durable-operation
anchor and MUST no longer be enqueued or own admission/execution resources on
the retired bridge. A Codex-affinity session MUST NOT qualify under this rule.

Replacement MUST retain the original account, request context, API-key
reservation and absolute deadline. An unavailable or conflicting owner MUST
fail without dispatch to another account or a raw-HTTP fallback. Cancellation
or deadline expiry MUST prevent replacement dispatch. Repeated retirement MUST
fail without another replacement. Existing hard gate-timeout replacement
behavior MUST remain unchanged.

#### Scenario: A waiting unsubmitted turn outlives its bridge
- GIVEN an unanchored soft prompt-cache turn waits behind an earlier execution
- WHEN the bridge closes or starts draining before the waiting turn can send
- THEN the proxy sends that waiting turn once on a replacement for the same account
- AND the client receives its completed response without the local retirement error.

#### Scenario: Replacement owner is unavailable
- GIVEN an eligible unsubmitted waiter lost its bridge
- WHEN the original account cannot provide a replacement
- THEN no other account or raw-HTTP path receives the turn.

#### Scenario: The replacement also retires
- GIVEN one replacement was already selected for the unsubmitted waiter
- WHEN that replacement retires before dispatch
- THEN the proxy fails without selecting a third connection.

#### Scenario: Upstream work or continuity may already exist
- GIVEN the turn has an attempted send, accepted response, output or continuity anchor
- WHEN execution admission refuses a retired bridge
- THEN this replacement rule MUST NOT dispatch the turn again.
