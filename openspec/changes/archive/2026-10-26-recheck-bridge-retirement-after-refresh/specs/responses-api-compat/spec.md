## ADDED Requirements

### Requirement: Bridge reuse rechecks retirement after lease refresh

When the canonical session-key registry reuse branch awaits durable lease refresh for its existing candidate, it MUST recheck retirement before returning the session to a request that does not own a reserved unanchored handoff. If the session became retiring during refresh, lookup MUST use its existing replacement selection and ownership rules instead of handing that session to the unsubmitted request. Replacement MUST preserve client-supplied `previous_response_id`, original input, API-key scope, file/account pins, and hard-owner restrictions. It MUST NOT replay an already dispatched request or migrate pending siblings to a new socket.

An existing reserved unanchored handoff MUST retain its existing authority to submit on its detached generation. The final pre-dispatch retirement fence MUST remain in force for retirement that begins after lookup returns.

#### Scenario: An older response detaches during refresh

- **GIVEN** a public Responses request is reusing a soft prompt-cache bridge
- **AND** an older accepted response detaches while lookup awaits durable refresh
- **WHEN** refresh returns with the bridge marked retiring
- **THEN** lookup selects a replacement through its normal ownership path
- **AND** the new request is sent exactly once on the replacement and never on the retiring socket

#### Scenario: Client anchor remains pinned during replacement

- **GIVEN** the incoming request supplies a previous response id owned by an account
- **WHEN** its candidate bridge becomes retiring during refresh
- **THEN** replacement preserves that previous response id and requires its owner account
- **AND** an unavailable required owner fails closed rather than switching accounts or dropping the anchor

#### Scenario: Reserved unanchored handoff retains its lane

- **GIVEN** the lookup reserved an unanchored handoff on a bridge
- **WHEN** refresh observes that its predecessor is retiring
- **THEN** lookup preserves that handoff's existing authority and does not silently move the reserved request to another generation

#### Scenario: A visible sibling still owns the retiring socket

- **GIVEN** another accepted visible response remains pending when an older request detaches during refresh
- **WHEN** lookup replaces the retiring session for an unsubmitted request
- **THEN** the sibling retains its pending ownership and physical socket without an injected terminal failure
- **AND** the detached predecessor drains through its existing lifecycle instead of being forcibly closed by replacement
