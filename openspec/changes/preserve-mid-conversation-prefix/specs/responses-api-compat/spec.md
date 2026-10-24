## ADDED Requirements

### Requirement: Replayed instruction messages preserve the context prefix
For Responses and compact requests supplied through `input`, the service SHALL hoist only leading instruction messages into top-level instructions. It SHALL preserve later instruction messages in their input position and SHALL normalize their system role to developer without changing the caller's input. Preserved non-message directives SHALL retain their existing behavior, and Responses Lite input SHALL remain unchanged. Existing compact truncation and replay-safety transformations remain separate from this normalization requirement.

#### Scenario: Tool history gains a token-budget reminder
- **GIVEN** an input with leading instructions followed by user and tool history
- **WHEN** another system reminder is appended to that history
- **THEN** top-level instructions remain identical
- **AND** previously serialized input remains a prefix of the new input
- **AND** the new reminder is retained as a developer message

#### Scenario: Mixed leading instruction content starts the input
- **GIVEN** leading instructions containing text and a non-text attachment
- **WHEN** normalization retains the attachment in a user input item
- **THEN** subsequent instruction messages remain after that item

#### Scenario: HTTP Responses compatibility routes preserve placement
- **WHEN** a streamed or collected Responses request reaches a subscription upstream through /responses or /v1/responses
- **THEN** initial instructions remain separate from later reminders
- **AND** tool calls, outputs, message IDs, and multimodal content remain in order
