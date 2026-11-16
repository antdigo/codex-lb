# Session/cache affinity ownership implementation plan

> Implement inline using superpowers:executing-plans in the existing isolated worktree. No commits/push without explicit authorization.

**Goal:** Close the two verified Important review findings with minimal coherent ownership contracts.
**Architecture:** Existing response snapshot fields gain exact response/owner guards and atomic reset; logical operations carry effective cache identity through physical attempts and compatible handshake reuse.
**Tech Stack:** Python3.13, pytest with early Landlock isolation, existing WebSocket/controller/pool and pinned Bifrost2.5.1.
**Spec:** design.md and specs/responses-api-compat/spec.md in this change.

## Global constraints
- Tests always use -c pyproject.toml -p tests.isolated_bootstrap; no production data/config/service operations, live provider probes or Git integration.
- Existing deadline, account/API/file/owner, publication and retry quota remain authoritative.
- No DB schema, dependency, setting, pool width or new cache/index. Do not rewrite either sealed held candidate.
- Existing explicit non-goal: no direct downstream WebSocket cache-identity redesign.

## Review focus
- Projected public response IDs may name the same exact physical snapshot only through a valid existing binding.
- Durable manifests for latestB cannot authorize explicitA replay; unknown and known-empty obligations differ.
- Unproven/string/empty terminal input clears older fingerprints/counts.
- Policy change during recovery does not rescope canonical data; later independent reuse differs from hard continuation.
- Shared mode preserves wire fields; isolated body and process headers use the same account once, including long keys and native tool discovery.

## Task 1: I2 exact owned snapshot
Files: http_bridge/{continuation,streaming,upstream_events}.py, tests/integration/test_http_response_snapshot.py, tests/unit/test_http_response_snapshot.py. Reuse existing projection binding and response/owner fields. Promote existing REDs and add old/latest durable proof and binding tests; implement exact guards and terminal reset; run whole affected response/recovery tests. Record evidence in .superpowers/sdd/plan-session-cache-affinity/progress.md.

## Task 2: I1 HTTP identity decision
Files: thread_cache_identity.py, http_operation.py, _service/{support,http_bridge/*,streaming/operation_handoff,streaming/retry,websocket/mixin}.py as required by traced call boundaries, plus behavioral identity route/unit tests. One resolved policy is passed through existing request/controller metadata. Scope canonical header copies only for covered HTTP operations; match physical handshake mode for independent reuse and preserve hard owner semantics. Add RED matrix/policy/retry/handoff tests before implementation; run complete affected policy/egress suites.

## Task 3: verified replacement
Freeze app/tests, run complete affected core/HTTP suites and Bifrost/static/operator checks; request one fresh Astra/max review; fix verified blockers with RED/GREEN. Sync strict specs/archive. Seal a NEW source/runtime based on currently installed operation-lifecycle rollback runtime, audit every file, verify package under read-only isolation grant, write READY last. Provide manual install/rollback and measurement limits, no activation or Git mutation.
