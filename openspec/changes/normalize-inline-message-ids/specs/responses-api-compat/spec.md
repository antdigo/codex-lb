## ADDED Requirements

### Requirement: Classic subscription egress omits redundant completed plaintext message IDs

For classic HTTP Responses and compact requests, subscription wire serialization MUST omit only the top-level id from a message item with role assistant, status completed, and a nonempty content list whose entries are output_text objects with string text, when encrypted_content and encrypted_function_args are absent or empty. The inline-ID pass MUST preserve all other fields of the existing normalized wire copy and MUST NOT mutate caller input, owner-forwarding payloads, or replay-safety evidence.

#### Scenario: Full assistant message is replayed in an affected reused session
- **GIVEN** a completed assistant message with an item ID and inline output_text content
- **WHEN** classic subscription wire serialization prepares its full-history replay
- **THEN** the message item ID is absent
- **AND** message text, existing content-coercion behavior, encrypted reasoning, and tool call/output pairing remain unchanged

#### Scenario: Incomplete or non-plaintext shapes are preserved
- **GIVEN** an incomplete message, missing completion status, empty content, unknown content type, encrypted item, or explicit item reference
- **WHEN** it is serialized
- **THEN** its ID and remaining fields remain unchanged

#### Scenario: Direct WebSocket and Lite retain IDs
- **GIVEN** a direct client WebSocket frame or fresh replay, or a Responses Lite request including a metadata-only continuation
- **WHEN** its wire payload is prepared
- **THEN** its original message ID remains unchanged

#### Scenario: HTTP ownership survives wire normalization and state reconstruction
- **GIVEN** an original request carrying a message ID that will be omitted on the wire
- **WHEN** dispatch, reconnect, auth recovery, or trim/reprepare considers moving it to another account
- **THEN** original ownership evidence MUST remain available and MUST block an unproven account switch
- **AND** verified same-account replay MUST remain available
- **AND** only the existing proven account-neutral projection MAY clear that evidence
