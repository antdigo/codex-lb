## ADDED Requirements

### Requirement: Precreated HTTP bridge connection lifetime recovery

The HTTP bridge MUST recover at most once from an explicit
`websocket_connection_limit_reached` error of type `error` when the sole pending
turn has no previous-response or hard continuity anchor and has no
accepted response ID, response events, visible output or downstream sequence.
The error frame MUST NOT name an accepted response. Recovery MUST use a new
physical WebSocket on the same account and preserve original request context,
reservation and absolute deadline. An unavailable or conflicting owner MUST
fail without dispatch to another account. The lifetime error MUST NOT count as
an account quota or health failure. Other pending or draining turns MUST block
replacement. Accepted responses MUST NOT be replayed under this rule.
After the lifetime retry is used, a replacement transport failure MUST NOT
trigger any additional replay of that turn. Reconnect selection and the retry
dispatch MUST remain inside the retained absolute deadline; a detached or
draining consumer MUST NOT dispatch a retry.

#### Scenario: Lifetime expires before response acceptance
- GIVEN a complete HTTP Responses turn is refused by the connection lifetime limit before any response event
- WHEN its account is still available
- THEN the proxy closes the expired socket and sends exactly one same-account retry on a new socket
- AND the downstream receives the successful response without the recovered lifetime error.

#### Scenario: Response was already accepted
- GIVEN response.created or another accepted response identity has been observed
- WHEN a lifetime error arrives
- THEN the proxy forwards the terminal error without replaying the turn.

#### Scenario: Replacement also reaches a lifetime limit
- GIVEN one same-account lifetime recovery has already been attempted
- WHEN the replacement receives the same error
- THEN the proxy forwards failure without a third dispatch.

#### Scenario: Required account is unavailable
- GIVEN the original turn was refused before acceptance
- WHEN its required account cannot provide a replacement connection
- THEN the proxy fails without selecting another account for dispatch.

#### Scenario: Replacement closes without accepting the retry
- GIVEN one lifetime retry has already been sent
- WHEN the replacement connection closes before response acceptance
- THEN the proxy fails without a third dispatch.

#### Scenario: Deadline or request ownership is lost while reconnecting
- GIVEN a lifetime retry is preparing its replacement connection
- WHEN the retained deadline expires or the consumer detaches or starts draining
- THEN the proxy does not send the retry on the replacement connection.
