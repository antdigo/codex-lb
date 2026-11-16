## ADDED Requirements

### Requirement: Destructive test fixtures reject unintended databases
The test harness SHALL capture its intended test database URL before importing application modules. It SHALL reject an already initialized engine or reset connection whose effective database differs from that target, before opening the engine connection or executing destructive schema statements. SQLite reset targets SHALL reside in disposable temporary storage and SHALL NOT resolve to the application's default data directory. Refusal errors SHALL NOT reveal database credentials.

#### Scenario: Application engine imported before the test harness
- **GIVEN** an application engine initialized against a different disposable sentinel database
- **WHEN** the test harness configures its intended test database
- **THEN** it refuses setup and preserves the sentinel schema and data

#### Scenario: A different connection reaches schema reset
- **GIVEN** the harness has a valid intended test target
- **WHEN** a reset is requested using a connection to another disposable sentinel database
- **THEN** it refuses before schema mutation and preserves the sentinel schema and data

### Requirement: Local verification excludes production storage
The local isolated test launcher SHALL set a temporary home and explicit test database before importing pytest or the application. It SHALL enforce kernel filesystem restrictions excluding production data, configuration and backups, inherited by child processes. It SHALL fail closed when the required restrictions cannot be enforced. Its denial checks SHALL use a disposable external canary for write, truncate and unlink attempts; production probes SHALL never attempt writes or read file contents.

#### Scenario: Verification starts without supported containment
- **GIVEN** the required kernel containment is unavailable
- **WHEN** the isolated launcher starts
- **THEN** it fails before importing pytest or application modules

#### Scenario: A test accidentally names a production database file
- **GIVEN** local tests run through the isolated launcher
- **WHEN** code attempts to open production storage
- **THEN** the kernel refuses access regardless of application settings

### Requirement: Native pytest containment precedes application plugins
The native pytest bootstrap SHALL enforce the same temporary-environment and kernel filesystem restrictions at explicit plugin import, before application modules, external plugins or conftest load. It SHALL refuse an already imported application, nonempty PYTEST_ADDOPTS or plugin loads in project addopts. Its supported invocation SHALL place it first among explicit plugins and use trusted Python/pytest startup. Each worker SHALL use temporary storage within inherited writable roots. Test results SHALL remain genuine pytest results; no wrapper test SHALL reinterpret a launcher failure as a successful regression run.

#### Scenario: Bootstrap starts with an imported application
- **GIVEN** an application module is already imported
- **WHEN** the native bootstrap loads
- **THEN** it refuses execution before collecting application tests

#### Scenario: Parallel test workers start
- **GIVEN** containment is active in the parent pytest process
- **WHEN** a worker loads the native bootstrap
- **THEN** it preserves inherited restrictions and creates separate temporary home, database and pytest storage
