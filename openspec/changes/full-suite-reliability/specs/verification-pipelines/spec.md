## ADDED Requirements

### Requirement: Manual CI exercises the full candidate
A manual CI workflow dispatch SHALL select every change area and run frontend coverage. The PostgreSQL job SHALL include runtime-gated live-usage identity, SQL-shape and migration-benchmark tests. Helm verification SHALL execute the Python chart contracts with Helm installed, in addition to lint, template and schema validation. Unit verification SHALL install the metrics optional dependency so its supported integration does not skip silently.

#### Scenario: A candidate is dispatched without a pull request diff
- **GIVEN** an operator starts the CI workflow manually for a candidate ref
- **WHEN** change areas are selected
- **THEN** every area is enabled and the coverage job runs

#### Scenario: PostgreSQL-only regressions exist outside decorator gates
- **GIVEN** identity and SQL tests skip at runtime under SQLite
- **WHEN** PostgreSQL verification runs
- **THEN** its explicit test selection includes those tests and the migration benchmark

#### Scenario: Helm source contracts are verified
- **GIVEN** Helm is installed in the CI chart job
- **WHEN** chart verification runs
- **THEN** the Python chart contracts execute alongside Helm lint, rendering and kubeconform
