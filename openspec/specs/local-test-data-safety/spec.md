# local-test-data-safety Specification

## Purpose

Protect production storage from accidental test-fixture access while preserving isolated local verification and genuine pytest results.

## Requirements

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

### Requirement: Explicit native test executable stays inside containment
The early pytest bootstrap SHALL accept `--native-egress-test-binary` as the sole explicit CLI selection for the native wire helper. It SHALL resolve symlinks and require an executable regular file within the checkout before preserving that selection in the scrubbed test environment. Selection SHALL NOT enlarge filesystem or network grants. Invalid selections SHALL fail before application imports.

#### Scenario: Native helper symlink escapes the checkout
- **GIVEN** a helper path inside the checkout resolving to a file outside it
- **WHEN** the bootstrap validates the selection
- **THEN** it refuses execution without granting access to the target

#### Scenario: Owned native wire helper runs
- **GIVEN** an executable helper inside the contained checkout
- **WHEN** native wire tests select it explicitly
- **THEN** the actual helper runs under inherited restrictions
- **AND** child test workers retain the same validated selection

### Requirement: Test readers and queries have an explicit owner
The test harness SHALL detect HTTP bridge reader tasks surviving their originating test's normal fixture shutdown, settle those tasks before another test reuses its database, and report the originating test as failed. Dashboard render helpers SHALL own their QueryClients; teardown SHALL unmount components, cancel pending reads and clear caches before resetting HTTP mocks.

#### Scenario: A bridge reader outlives its test
- **GIVEN** an HTTP bridge reader remains pending after transport and application teardown
- **WHEN** the test ownership fence runs
- **THEN** it cancels and awaits that reader
- **AND** reports the originating test and reader ownership

#### Scenario: A dashboard read is still pending at teardown
- **GIVEN** a helper-owned QueryClient has a pending read
- **WHEN** its test finishes
- **THEN** teardown cancels that read and clears its cache before the next handler set

### Requirement: Sequential WebSocket reuse fixtures preserve dispatch causality
Tests asserting sequential request reuse SHALL release each synthetic response batch only after its corresponding response.create is sent. They SHALL cover both immediate follow-up after the first terminal and follow-up with the reader already awaiting its next batch. Fakes used deliberately to test unsolicited or stale frames MAY retain explicit preloaded-frame behavior.

#### Scenario: Two sequential requests share an upstream connection
- **GIVEN** a fake upstream has distinct first and second response batches
- **WHEN** only the first response.create has been sent
- **THEN** the second response batch is not available to its reader
- **AND** the second response batch is released by the second actual response.create

### Requirement: Shared cleanup deadline unit tests use causal virtual time
Unit tests of cleanup ordering and task ownership at a shared shutdown deadline SHALL use injected clocks and schedulers. They SHALL verify that waiting remains pending immediately before the original deadline and finishes at that deadline even when a caller offers a longer timeout. Terminal work delegated by the cancelled reader SHALL remain uncancelled and drain after explicit release. Host scheduling latency and delayed rescue tasks SHALL NOT decide those assertions.

#### Scenario: An owned terminal child outlives the shared deadline
- **GIVEN** terminal work is held by an explicit release event and the caller offers a longer timeout
- **WHEN** virtual time reaches the shared shutdown deadline
- **THEN** the cancellation waiter finishes without cancelling its owned child
- **AND** releasing that child allows persistence drain to complete

### Requirement: Concurrent selection tests prove nonserialized persistence
Account-selection concurrency unit tests SHALL hold persistence behind an explicit barrier and prove that all concurrent selections reach that barrier before any persistence completes. They SHALL verify that the shared runtime lock is free while persistence is blocked and settle every created selection task on both success and failure. A liveness timeout MAY detect deadlock; host wall-clock throughput SHALL NOT substitute for the concurrency assertion.

#### Scenario: One hundred selections persist independently
- **GIVEN** one hundred concurrent selections and a blocked persistence boundary
- **WHEN** no persistence has been released
- **THEN** all one hundred selections enter persistence without holding the shared runtime lock
- **AND** a serialized selection path cannot satisfy the barrier

### Requirement: SSE keepalive timing tests use the shared virtual timer seam
SSE integration harnesses asserting heartbeat ordering SHALL drive the existing shared-future scheduler seam with virtual time while retaining the real injector, stream normalization and ownership cleanup. They SHALL hold upstream data with explicit events, distinguish an immediate native heartbeat from a timed idle heartbeat, and verify no timed heartbeat before its configured interval. A subsecond host wait SHALL NOT decide those ordering assertions. The fixture SHALL be opt-in and settle its owned read tasks during teardown.

#### Scenario: A source stays silent through its first heartbeat interval
- **GIVEN** a real keepalive injector wrapping an explicitly held upstream and a virtual timer
- **WHEN** time is advanced to just before and then to the configured interval
- **THEN** the read remains pending before the interval and yields the heartbeat at the interval
- **AND** releasing upstream data preserves response-created-before-completed ordering
