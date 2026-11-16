## ADDED Requirements

### Requirement: Lazy API key dialogs suspend locally
Loading a closed API key create or edit dialog SHALL NOT suspend the Settings page or hide the API key controls. The lazy dialogs SHALL share a local loading boundary. Once loaded, their existing create, edit and delete interactions SHALL remain available.

#### Scenario: A closed dialog chunk is still loading
- **GIVEN** the Settings page mounts while an API key dialog import is pending
- **WHEN** the dialog is closed
- **THEN** the Create key control and the surrounding Settings page remain rendered
