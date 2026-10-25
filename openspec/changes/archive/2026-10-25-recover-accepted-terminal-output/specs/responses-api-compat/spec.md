## ADDED Requirements

### Requirement: Output-free accepted public streams recover owned terminal items

When public HTTP Responses normalization has emitted a valid `response.created` envelope but no response output events, and receives a `response.completed` or `response.incomplete` envelope with the same response id and nonempty output, it MUST expose the terminal output through complete output-item and content/tool-argument lifecycle events before the terminal event. It MUST emit each terminal output item exactly once, retain exactly one `response.created`, and use strictly increasing sequence numbers for the recovered lifecycle and terminal after the accepted prelude. It MUST NOT synthesize that complete output lifecycle for a different terminal response id, duplicate previously emitted output, or change native Codex passthrough behavior.

#### Scenario: Creation and metadata precede terminal-only output

- **GIVEN** a public stream emits creation, in-progress, and metadata for response A, with no output events
- **WHEN** a completed terminal for A contains reasoning, message text, and function-call arguments
- **THEN** each item is opened before its content and closed before completion
- **AND** streaming consumers receive message text and function-call arguments once within A's sole lifecycle

#### Scenario: Partial output suppresses full terminal replay

- **GIVEN** the accepted stream already emitted any output event, including a function-argument or reasoning delta
- **WHEN** its terminal envelope includes the full output
- **THEN** the service does not synthesize the full output lifecycle again

#### Scenario: Different terminal owner is not recovered

- **GIVEN** a stream accepted response A without output
- **WHEN** a terminal envelope names response B
- **THEN** the service does not synthesize B's output-item lifecycle into A

#### Scenario: Native streams preserve upstream events

- **GIVEN** native Codex passthrough disables the public SDK contract
- **WHEN** an output-free accepted stream receives a full terminal response
- **THEN** the stream does not gain synthesized item or argument events

#### Scenario: Duplicate terminal and late output do not replay a finished response

- **GIVEN** a public stream has emitted its terminal response and recovered its owned items
- **WHEN** another completed or incomplete event or a late output event arrives
- **THEN** the public stream exposes only the first terminal response
- **AND** no additional output item, text, or tool-argument event is emitted after that terminal
