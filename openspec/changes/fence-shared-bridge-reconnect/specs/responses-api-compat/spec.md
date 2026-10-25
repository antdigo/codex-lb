## ADDED Requirements

### Requirement: Automatic bridge socket replay preserves every pending owner

When an HTTP bridge transport ends while several requests remain pending, automatic pre-created replay MUST NOT replace the shared socket for only one request. This restriction MUST include siblings that already received `response.created` and siblings detached from their downstream consumer but still draining until a terminal event. Refusal MUST occur before replay state mutation, create-gate reclamation, or a replacement send. The reader failure funnel MUST terminally settle every request that remains owned by the failed socket exactly once. A sole eligible pending request MUST retain the existing bounded replay behavior.

Every bridge reconnect entry MUST also reject replacement before account selection or connection mutation when a different request remains pending. A prewarm timeout MAY explicitly discard its own identified warmup request during replacement, but MUST NOT discard any other pending owner.

#### Scenario: Pre-created retry has an accepted sibling

- **GIVEN** an HTTP bridge has an accepted pending response and one retryable request awaiting `response.created`
- **WHEN** the upstream transport ends
- **THEN** the bridge does not reconnect and resend the pre-created request alone
- **AND** both requests receive the existing terminal stream failure and are settled exactly once

#### Scenario: Pre-created retry has a draining sibling

- **GIVEN** an HTTP bridge has one retryable pre-created request and another pending request draining after downstream cancellation
- **WHEN** the upstream transport ends
- **THEN** the bridge refuses to replace the shared socket for the pre-created request alone
- **AND** the draining request is finalized with the other pending request

#### Scenario: Sole pre-created request retains bounded recovery

- **GIVEN** a failed HTTP bridge socket has exactly one eligible pending request
- **WHEN** its existing replay policy permits recovery
- **THEN** the bridge may reconnect and resend it within the existing replay bound

#### Scenario: Unsubmitted recovery cannot replace another response's socket

- **GIVEN** an unsubmitted request attempts fresh-upstream recovery while another response remains pending
- **WHEN** it reaches bridge reconnect
- **THEN** reconnect fails before account selection or opening another socket
- **AND** the other response is not migrated to a socket on which it was never sent

#### Scenario: Prewarm timeout has another accepted owner

- **GIVEN** a warmup and another accepted response remain pending on a bridge
- **WHEN** the warmup times out
- **THEN** the bridge does not replace the other response's socket for the warmup
