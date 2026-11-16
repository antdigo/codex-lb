## ADDED Requirements

### Requirement: Startup cancellation retains the service iterator owner
The HTTP route SHALL retain ownership of its service iterator until the delivery cleanup owner receives it. If startup probing is interrupted after its first-item task completes, the route SHALL close the original iterator before releasing startup-owned reservations. Closing operation authority SHALL precede waiting for a suspended replacement reader, and no replacement response.create SHALL be dispatched after cancellation.

#### Scenario: Cancellation between a completed startup probe and delivery handoff
- **GIVEN** the probe has consumed the first lifecycle item and a replacement reader is suspended
- **WHEN** downstream cancellation interrupts the handoff to the response writer
- **THEN** the original service iterator is closed by the startup owner
- **AND** operation authority closes before reader cleanup can finish

### Requirement: Portable discovery fixtures preserve references without operator content
The offline fixture sanitizer SHALL rebuild client tool-search calls and outputs using the reviewed input field vocabulary, preserve pairing between discovery calls and outputs and between loaded tools and subsequent calls, and replace operator-authored names, arguments and descriptions deterministically. Rebuilding its output SHALL be idempotent. Unreviewed discovery fields SHALL be refused. Client function and client tool-search parameter schemas SHALL remain inert declarations; hosted and unknown tool state SHALL retain owner-sensitive classification.

#### Scenario: Discovery loads a client function used by the next call
- **GIVEN** paired client discovery items and a later call to an operator-named loaded function
- **WHEN** the offline sanitizer rebuilds the body
- **THEN** output and call reference the same anonymized function name and paired call identifiers
- **AND** operator-authored argument values and descriptions are absent
- **AND** the rebuilt request retains canonical input completeness

### Requirement: String input retains advisory warm-lane locality
Public string input SHALL normalize to the canonical itemized representation before ordinary owned snapshot and cache locality processing. Exact repeats SHALL prefer the matching idle warm lane using existing proof. Locality scoring SHALL NOT grant additional authority to trim input, inherit a continuation or change account.

#### Scenario: Two independent string requests warm separate lanes
- **GIVEN** completed string requests A and B on two compatible logical lanes
- **WHEN** B is repeated after both lanes become idle
- **THEN** the lane warmed by B is preferred
- **AND** the full canonical wire input is retained without an inherited anchor

### Requirement: Discovery fixture rebuilding retains invalidity and supported flags
Offline sanitization SHALL preserve canonical completeness and portability rejection for missing, null, empty and whitespace-only discovery names and call identifiers. Supported boolean deferred-loading declarations SHALL remain represented with their original boolean value; unreviewed fields and invalid deferred-loading values SHALL be refused.

#### Scenario: Discovery names or pairing identifiers are blank
- **GIVEN** otherwise paired client discovery with an invalid blank function name or call identifier
- **WHEN** the body is anonymized and serialized
- **THEN** rebuilding SHALL NOT convert it to complete portable input

#### Scenario: A loaded client function is deferred
- **GIVEN** a valid loaded function with a boolean defer_loading field
- **WHEN** the discovery body is anonymized and rebuilt again after serialization
- **THEN** its field value, identity pairing and classifier results remain unchanged
