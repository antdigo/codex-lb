## ADDED Requirements

### Requirement: Shared cleanup deadline unit tests use causal virtual time
Unit tests of cleanup ordering and task ownership at a shared shutdown deadline SHALL use injected clocks and schedulers. They SHALL verify that waiting remains pending immediately before the original deadline and finishes at that deadline even when a caller offers a longer timeout. Terminal work delegated by the cancelled reader SHALL remain uncancelled and drain after explicit release. Host scheduling latency and delayed rescue tasks SHALL NOT decide those assertions.

#### Scenario: An owned terminal child outlives the shared deadline
- **GIVEN** terminal work is held by an explicit release event and the caller offers a longer timeout
- **WHEN** virtual time reaches the shared shutdown deadline
- **THEN** the cancellation waiter finishes without cancelling its owned child
- **AND** releasing that child allows persistence drain to complete

### Requirement: Sequential WebSocket reuse fixtures preserve dispatch causality
Tests asserting sequential request reuse SHALL release each synthetic response batch only after its corresponding response.create is sent. They SHALL cover both immediate follow-up after the first terminal and follow-up with the reader already awaiting its next batch. Fakes used deliberately to test unsolicited or stale frames MAY retain explicit preloaded-frame behavior.

#### Scenario: Two sequential requests share an upstream connection
- **GIVEN** a fake upstream has distinct first and second response batches
- **WHEN** only the first response.create has been sent
- **THEN** the second response batch is not available to its reader
- **AND** the second response batch is released by the second actual response.create

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
