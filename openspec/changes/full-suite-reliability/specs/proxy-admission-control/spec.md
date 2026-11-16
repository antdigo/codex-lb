## ADDED Requirements

### Requirement: Create-owner settlement receipts confirm completed release
An HTTP operation SHALL mark its create-owner cleanup receipt complete only after the retained account-create lease is actually released. A failed release SHALL remain retryable by ordinary compensation while independent execution gates, stream leases, reservations and terminal delivery continue settling. Retrying SHALL NOT release an already released semaphore or settle unrelated resources twice.

#### Scenario: Account-create release fails once during terminal cleanup
- **GIVEN** an HTTP operation retaining an account-create lease whose release fails transiently
- **WHEN** terminal cleanup runs and its downstream then detaches
- **THEN** the create-owner receipt remains incomplete until the detach retries that release successfully
- **AND** independent resources and terminal delivery settle once
