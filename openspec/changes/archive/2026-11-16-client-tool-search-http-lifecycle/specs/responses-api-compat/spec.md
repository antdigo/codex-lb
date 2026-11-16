## ADDED Requirements

### Requirement: Complete client tool-search history uses the HTTP operation contract
The proxy SHALL recognize a complete native client-executed tool-search call and matching output as client work for existing HTTP pool admission and pre-publication recovery, while retaining the original native JSON, loaded tool definitions and prompt-cache identity.

#### Scenario: Bifrost replays completed client tool discovery
- **WHEN** the installed Bifrost sends a complete unanchored native history with paired `execution=client` tool-search items and client-only tool definitions
- **THEN** the request uses the existing bounded operation contract and may recover from a retired predecessor or an eligible pre-publication transport failure on the same account within its original deadline and quotas
- **AND** no old-attempt output or duplicate response is published

#### Scenario: Discovery does not prove replay safety
- **WHEN** a history has hosted or unspecified search execution, an unresolved or mismatched pair, malformed loaded tools, opaque references, or published output
- **THEN** the proxy does not obtain a complete-client-work replay grant merely because the request contains tool search
