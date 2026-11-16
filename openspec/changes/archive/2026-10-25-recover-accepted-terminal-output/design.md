## Context

See proposal.md for the missing lifecycle. The existing terminal renderer can expose complete owned output safely; anonymous orphan deltas cannot supply equivalent ownership proof.

## Decisions

- Retain the accepted response id and whether output has been emitted. Recover only a nonempty, matching completed/incomplete envelope before any output, reusing `_terminal_output_lifecycle_events`.
- Keep one creation envelope, preserve the complete terminal response, and assign increasing sequence numbers to generated frames and the terminal.
- Suppress public data after the first terminal so duplicate completion or late argument frames cannot replay tool output. Native passthrough keeps its existing behavior.
- Do not reconstruct partially delivered output: without an exact delivery ledger that would risk duplicate text or side effects. Existing legacy fallbacks remain outside this complete-lifecycle recovery.

## Risks and rollout

Recovery emits output at completion rather than restoring live token timing. It cannot recover a truncated stream with no authoritative terminal output. Per-stream state is constant sized and buffers no additional output. Benchmark normal streaming separately from recovery. Ship as Python-only code under normal deployment draining; no migration or native helper rebuild. This investigation does not deploy it.
