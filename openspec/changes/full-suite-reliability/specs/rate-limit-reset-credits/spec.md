## ADDED Requirements

### Requirement: Heartbeat cancellation preserves transaction ownership
Cancelling the reset-credit claim heartbeat SHALL wait for its current renewal transaction to finish and close its session before cancellation propagates. Cancellation SHALL NOT leave an unfinished SQLite RETURNING cursor retaining the database writer lock. Existing holder checks, lease duration and conditional takeover semantics SHALL remain unchanged. A simultaneous renewal failure SHALL NOT swallow cancellation or start another heartbeat transaction.

#### Scenario: Cancellation arrives during a renewal statement
- **GIVEN** the heartbeat has begun its dedicated renewal transaction
- **WHEN** its owner cancels the heartbeat before the statement result is consumed
- **THEN** transaction completion and session cleanup remain owned and awaited
- **AND** an independent session can write after the heartbeat finishes

#### Scenario: Renewal fails while cancellation is pending
- **GIVEN** the owner cancelled a heartbeat with an in-flight renewal
- **WHEN** that renewal fails before returning
- **THEN** the owner still observes cancellation after transaction cleanup
- **AND** no subsequent heartbeat transaction starts
