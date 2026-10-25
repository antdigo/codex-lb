## ADDED Requirements

### Requirement: Bounded unowned prelude does not abort an owned response
The public HTTP Responses stream's pre-created replay buffer SHALL retain at
most 64 event payloads. On overflow it SHALL discard all retained unowned payloads, SHALL NOT
retain further pre-created payloads, and SHALL continue awaiting an owned response
envelope within the existing upstream request budget. It SHALL NOT fabricate a
stream failure solely because this retention cap was reached.

#### Scenario: Terminal envelope follows an oversized orphan prelude
- **GIVEN** at least 65 events arrive without a response envelope
- **WHEN** a valid completed or incomplete response envelope subsequently arrives
- **THEN** the service SHALL consume and expose that terminal response
- **AND** the first public Responses event SHALL be response.created
- **AND** no orphan text, tool arguments or items SHALL be attached to it
- **AND** owned terminal output SHALL be available to streaming SDK clients,
  including message text and function-call arguments, exactly once

#### Scenario: A real created event follows an oversized orphan prelude
- **WHEN** a real response.created follows at least 65 unowned events
- **THEN** the service SHALL emit it and subsequently owned stream events normally
- **AND** it SHALL NOT replay discarded pre-created events

#### Scenario: No owned terminal response arrives
- **WHEN** an oversized prelude ends without a valid terminal response
- **THEN** the service SHALL expose a failure rather than a successful completion

#### Scenario: Explicit upstream failure and cancellation remain failures
- **WHEN** the stream explicitly fails or the consumer cancels
- **THEN** the service SHALL preserve error or cancellation semantics and cleanup
- **AND** it SHALL NOT invent a successful response or dispatch a retry
