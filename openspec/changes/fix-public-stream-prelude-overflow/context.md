# Design and implementation plan

Goal: Claude Desktop can complete a valid response after a long pre-created
prelude without mixing unowned output from cancelled requests into that response.

Alternatives considered: raising the buffer cap retains more unowned data and
only moves the failure threshold; immediately attaching orphan items guesses
ownership. Discarding the bounded prelude while awaiting an owned envelope keeps
the memory bound and preserves ownership checks. This is the implementer's choice
under the user's standing instruction to act autonomously.

## Plan

1. Add failing unit and public-route regressions for 65+ pre-created events
   followed by created/completed/incomplete/error envelopes. Assert final text,
   function arguments, SDK parsing, no orphan replay, true EOF failure, source
   cancellation and native Codex passthrough isolation.
2. Keep at most 64 pre-created payloads, clear and permanently discard the
   prelude after overflow. Continue normal stream processing on the first owned
   envelope. Reconstruct only output contained in a trusted terminal envelope
   when the prelude was discarded and no owned item lifecycle was emitted.
3. Run focused red/green, the affected API suites, lint/type checks, strict
   OpenSpec validation and the project pytest command; report existing unrelated
   failures explicitly. Have a fresh reviewer inspect ownership, completion,
   tool-call duplication, retained memory and request finalization.
4. Build a new immutable runtime copied from the active message-ID runtime,
   replace only verified source modules, remove candidate bytecode caches, seal
   with source/test hashes. Exercise it through an isolated official Bifrost
   against a local mock, including tool calls. Prepare reversible activation and
   rollback with readiness and running-executable checks.

## Review focus

- A valid terminal after an arbitrarily long orphan prelude must be consumed.
- Orphan text/calls must never be emitted or used to backfill owned output.
- Native owned text and function arguments must arrive exactly once in SDK and
  Anthropic/Bifrost consumers, even when only terminal output is available.
- EOF/explicit errors and cancellation must not become successful responses.
- Existing under-limit legacy text, normal streams, native Codex passthrough,
  cache affinity and account/usage settlement must keep their contracts.

## Operational limits

No external replay is needed to prove the regression: deterministic streams can
model a late envelope. Local tests do not prove the exact lost native prelude of
the original incident. Existing upstream request/idle budgets remain in force.
Retained unowned state stays capped at 64 payloads and becomes empty on overflow.
Only the abnormal recovery path constructs terminal item lifecycle events.

## Execution ledger

Plan: this file. Baseline HEAD 20e1ec319d02f23a36060deab2ea9773ce2264cf.
Ruling: use the existing dedicated /tmp checkout and compatibility branch; the
workspace Bifrost tree and active runtime remain untouched during development.
Ruling: repository OpenSpec owns the design/plan instead of a second docs tree.
Pre-flight: tests, source normalization and runtime packaging depend on the same
public SSE contract; packaging starts only after verified source and test hashes.

Verification: six new unit cases and both public-route SDK cases reproduced the
old premature failure before implementation. The complete affected selection
passed 170 tests. A fresh reviewer found a missing function-call name on the
arguments-done event and a reasoning-content/text fallback conflict; both gained
failing regressions and were corrected before the final affected run. Whole-tree
Ruff checks, formatting, architecture checks and type checking pass.
After adding explicit JSON types and runtime type assertions to the new tests,
all 14 recovery regressions passed again (67 unrelated cases deselected).

Full pytest run: 8697 passed, 57 failed, 108 skipped, one teardown error under
`-n 4 --dist=loadfile --timeout=45`. Failures: model-source routing (45), HTTP
client unit tests (3), HTTP bridge (1), sticky sessions (1), missing built frontend
assets (1), HTTP upgrade (1), process shutdown (5); the retry test timed out during
teardown. Shutdown errors identify the environment's SOCKS proxy without socksio.
The remaining failures have not all been reproduced on the unchanged base; this
run is not a green full-suite result. The local-ci hook bootstrap could not fetch
its Ruff environment through the unavailable configured proxy. Equivalent local
Python lint/type/architecture checks were run directly. This change passes strict
OpenSpec validation; unchanged canonical specifications have 55 strict failures.

The candidate retains all eight existing compatibility modules and changes only
the public API module. Its source and QA hashes were sealed. A local official
Bifrost v2.3.2 fixture exercised both the saved request against a mock upstream
and Anthropic-to-Responses conversion: owned text, reasoning summary, encrypted
state and function arguments survived exactly once. No external model calls were
made. Nine activation/rollback fault scenarios passed, including HUP and recovery
after an untrappable kill. The independent reviewer approved code and activation
changes after the findings were addressed.

The original incident's native upstream prelude was not recorded. The synthetic
regression reproduces its public error envelope, but does not prove that buffer
overflow was the only cause of that incident. Real EOF continues to fail closed;
the fix does not turn disconnected or failed upstream responses into success.
Live activation requires the machine owner's sudo password and is tracked
separately from source and local-pipeline verification.

Live activation completed on 2026-10-25 at 08:35:05 UTC. The running process uses
the pinned stream runtime; all nine source-module hashes match, readiness is
healthy, and systemd reports zero restarts. Official Bifrost v2.3.2 retained its
existing process. One real Anthropic-shaped request through Bifrost and codex-lb
to GPT returned HTTP 200, text OK and message_stop in 2.459 seconds; both services
recorded success (11 input tokens, zero cached tokens, five output tokens).
Continuation of the original failing Desktop session remains to be verified.
