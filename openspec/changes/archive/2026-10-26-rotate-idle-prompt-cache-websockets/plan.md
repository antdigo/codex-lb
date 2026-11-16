# Idle prompt-cache WebSocket lifetime implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans task-by-task. Steps use checkbox syntax.

**Goal:** Prevent the observed 3593.4-second idle unanchored PCK reuse failure while retaining useful within-connection prompt caching.
**Architecture:** Add bounded physical-age and reader-processing scalar state to the existing bridge session. Pass narrow full-context eligibility from streaming into canonical registry lookup; prune only an unowned target through existing replacement/epoch handling and require its old account end-to-end. Age checks do no payload walk on ordinary requests.
**Tech Stack:** Python3.13, asyncio/anyio, existing FastAPI/SQLAlchemy bridge tests.
**Spec:** specs/responses-api-compat/spec.md; context.md and proposal.md in this change.

## Global Constraints
Production remains2527 until a separately verified runtime is activated. No live services, Bifrost, dependencies, schema, native binary, remote pushes or model calls by implementer. User already authorized autonomous execution and opted out of design questions. Preserve all existing ownership, client anchors, ciphertext, PCK, settlement and finite-budget guarantees. Unknown eligibility skips rotation. Threshold 3000s is scoped prevention, not a guarantee for busy or hour-long generations. Use existing isolated worktree/branch fix/http-bridge-websocket-lifetime; do not create another worktree. Do not spawn subagents. Full-project green is not claimed; run appropriate affected checks, and never live paid provider harnesses.

## Review Focus
- Literal draining pending and post-pop terminal persistence must block age eviction.
- Parent/durable hydration must not put an old socket anchor into the fresh full-body send.
- Required account must survive create failure and outer HTTP fallback.
- Unknown/reference-only shapes and original anchors must skip rotation.
- Concurrent lookup/prepared owners and failed reconnect must not double-send or leak leases.

### Task 1: Guarded canonical idle PCK rotation
Files: support.py(session scalars), http_bridge/session_registry.py or small connection_lifetime.py(manager policy), mixin.py(create/reconnect/lookup wiring), streaming.py(capability and fallback), upstream_events.py(reader processing ownership), existing unit/public HTTP bridge tests. Keep helpers cohesive; no broad refactor of legacy modules to fit a budget.
Interfaces: implementer chooses private helper names and keyword-only defaults, records them in verification. Eligibility is bound to original target key; scope original+effective anchors/conversation/turn/session, forwarded/Lite/recovery must be excluded. Root-approved proof: original/effective unanchored Responses input defines the complete context; soft PCK is not implicit continuity. Require narrow complete inline items and uniquely paired calls/outputs, not a historical prefix or durable row; do not construct or send a projected history. Guard expensive shape checks behind actual age eligibility.
- [x] Write public route RED with two sequential same-PCK calls, owner clock offset 3593.4s, idleTTL 3600, fake old socket emits created/in_progress/reasoning/failed(limit) if reused; fresh fake socket completes. Watch terminal completed assertion fail before production edits.
- [x] Implement minimal tracking/policy and verify GREEN; add terminal-pop, draining, admission/reservation, explicit anchor/opaque, exact threshold/reset and account/fallback negative tests. No late accepted replay. Re-evaluate require_preferred_account after prune; no soft require_same_account shortcut.
- [x] Run focused and complete affected bridge unit/integration modules, lint/type/architecture/strict specs. Save concise durable logs outside checkout, archive verified OpenSpec change and commit locally. Report limits explicitly; no full-project CI claim.

## Verified review outcomes
The required account is retained through a request-state handle and enforced at central reconnect; higher-level owner-dropping recovery fails closed. An age predecessor is both immediately assigned a cancellation-safe cleanup owner and retained in the planned synchronous-close list. Both paths share the existing single-flight close, preserving capacity and cleanup on cancellation or owner lookup failure. All three independent-review findings were resolved before final verification.
