## ADDED Requirements

### Requirement: Classic subscription egress omits redundant inline function-call item ids

For classic HTTP Responses and compact requests, subscription wire serialization MUST omit id from a function_call item with a nonempty string call_id, nonempty string name and string arguments when encrypted_content and encrypted_function_args are absent or empty. Serialization MUST preserve call_id, name, arguments and every other field and MUST NOT mutate the request object or owner-forwarding payload. Direct client WebSocket requests and their fresh replay bodies MUST preserve the original ids and account-ownership classification.

#### Scenario: Replayed Bifrost function call is sent without a persisted-item reference
- **GIVEN** a complete plaintext function_call with id fc_call_example and call_id call_example followed by its function_call_output
- **WHEN** a classic Responses or compact request is serialized for the subscription backend
- **THEN** the function_call id is absent from the upstream payload
- **AND** call_id and its output pairing remain unchanged

#### Scenario: Cryptographic and reference items retain their ids
- **GIVEN** encrypted reasoning, an encrypted function call, an incomplete function-call shape, or an explicit item_reference
- **WHEN** the request is serialized
- **THEN** those items retain their ids and encrypted data unchanged

#### Scenario: Replay-safety classification retains ownership evidence
- **GIVEN** a request carrying an inline function-call id
- **WHEN** the request is rendered for replay-safety classification or owner forwarding
- **THEN** its original id remains available

#### Scenario: Responses Lite preserves its wire contract
- **GIVEN** a request containing an additional_tools bundle and a function_call id
- **WHEN** it is serialized
- **THEN** the function-call id remains unchanged

#### Scenario: Incremental Lite retains its function-call id
- **GIVEN** a forwarded Lite continuation carrying previous_response_id and the Lite client-metadata marker without an additional_tools bundle
- **WHEN** it is serialized
- **THEN** its function-call id remains unchanged

#### Scenario: Direct WebSocket dispatch and fresh replay preserve ownership
- **GIVEN** a direct client WebSocket request carrying a function-call id
- **WHEN** the initial frame or its fresh replay body is constructed
- **THEN** the id remains unchanged
- **AND** dispatch ownership remains pinned to the original account

#### Scenario: HTTP reconnect retains original ownership after wire normalization
- **GIVEN** a classic HTTP request whose complete function-call item carried an id
- **WHEN** the id is omitted on the wire and the request is considered for reconnect, auth recovery or account switching
- **THEN** recovery MUST retain the original account-ownership evidence
- **AND** ordinary prefix trimming and request-state reconstruction MUST NOT erase that evidence
- **AND** a verified same-account fresh replay remains available

#### Scenario: Proven account-neutral projection can clear original evidence
- **GIVEN** existing durable replay checks prove a projected plaintext full resend account-neutral
- **WHEN** the service activates that explicit projection
- **THEN** the projected request MAY clear the original function-call-id evidence
