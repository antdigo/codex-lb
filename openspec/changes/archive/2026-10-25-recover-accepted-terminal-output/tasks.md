## 1. Accepted response terminal recovery

- [x] 1.1 Add a failing public stream contract test for created/in-progress/metadata/completed with reasoning, message and function-call output; assert strict item ordering, complete arguments, one creation and increasing sequences.
- [x] 1.2 Reuse terminal lifecycle rendering only when no output was emitted and the accepted and terminal response ids match.
- [x] 1.3 Cover partial/already streamed output, mismatched response ownership, empty terminal output, incomplete responses, and native passthrough.
- [x] 1.4 Run contract/bridge regressions, broad tests, formatting/lint/types and strict OpenSpec validation before syncing and archiving.
- [x] 1.5 Prove and fence duplicate terminal and late-output events after public completion, preserving one terminal and one recovered item lifecycle.
