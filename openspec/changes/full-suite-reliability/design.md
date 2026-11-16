# Full-suite Reliability Implementation Plan

Goal: Explain and resolve every reported failure without weakening assertions, ownership or production-data containment.
Spec: existing responses-api-compat, outbound-http-clients, live-usage-ingestion, local-test-data-safety.
Global constraints: production databases/accounts/services untouched; no schema changes; no blanket timeout increases or new xfails; preserve session/cache ownership; commits/push/deploy separate.
Review focus: pre-head cancellation vs body idle; unsafe usage replay; discovery privacy and pairing; stale/ambiguous account fallback; task and query cleanup across tests.

### Task 1: Native/contained execution
Validate explicit native executable in existing permitted filesystem roots before restoring its environment selection after early containment. Native body probes use 5s HEAD with unchanged 0.1s idle and wait for actual headers before truncation. Separate head/cancellation regressions retain public behavior and peer helper usability.
### Task 2: Replay/privacy
Add strict client-search call/output sanitizer nodes, deterministic identifiers and loaded-function names. Retain privacy/idempotence/shape gates. Add semantic mismatched-response rejection and inert client schema counterexamples.
### Task 3: Backend/ownership
Fixed limiter time, SourceDispatch clock plus upstream-open barrier, explicit pre/post-keepalive errors, PG barrier after SELECT result with both consolidation orders. Use valid bridge session/service harness and synchronous POST context-manager double. Expose task leaks before cleanup and settle them before next DB fixture.
### Task 4: Frontend
Local Suspense for closed lazy dialogs; bounded owned query teardown before resetting MSW; mock missing cache plan; scoped selectors/paste; split JSDOM CRUD into independent cases with full real-API Playwright journey.
### Task 5: Full verification
Enable full workflow_dispatch (coverage/all filters), include remaining PG and Helm checks. Run affected red/green checks, then full suites on frozen source and fresh final review. Report skips and unresolved failures, preserve evidence, no automatic activation.
