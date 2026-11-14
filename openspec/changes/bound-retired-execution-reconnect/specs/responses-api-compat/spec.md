## ADDED Requirements

### Requirement: Execution waiter replacement consumes pre-dispatch reconnect budget

A turn transferred by the retired HTTP execution-waiter replacement rule MUST
NOT open another upstream WebSocket before its first actual response.create
attempt. Closure during replacement preparation or after admission MUST fail
through the existing cleanup paths without another connection, dispatch or
raw-HTTP fallback. The transfer MUST NOT consume or reset the replay counter
used by existing post-dispatch recovery policies.

#### Scenario: The selected replacement closes during preparation
- GIVEN an unsubmitted execution waiter selected its single replacement
- WHEN that replacement closes during a preparation await
- THEN submission fails without opening a third physical connection
- AND the existing request resources and API-key reservation are settled.

#### Scenario: The selected replacement closes after admission
- GIVEN the transferred waiter acquired admission on its replacement
- WHEN the replacement closes before response.create is sent
- THEN submission fails without reconnecting or sending the request
- AND admission and execution ownership are released.

#### Scenario: An actual send receives a recoverable connection lifetime error
- GIVEN the transferred waiter attempted response.create on its replacement
- WHEN the upstream reports an eligible pre-response connection lifetime error
- THEN the existing bounded same-account post-dispatch recovery policy remains in force.
