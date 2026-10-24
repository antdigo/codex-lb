## Why

Claude Code appends system reminders, including token-budget updates, inside replayed history. Responses normalization currently hoists every system/developer message into top-level instructions, changing the context before all old input and invalidating an otherwise append-only prompt-cache prefix.

## What Changes

- Hoist only the initial instruction-message prefix.
- Keep later instruction messages in input order, converting message role system to developer without mutating caller data.
- Preserve typed directives, Responses Lite, multimodal content, tool history, and existing compact behavior.
- Cover Responses/compact serialization and streamed/collected HTTP compatibility routes.

## Capabilities

### Modified Capabilities
- `responses-api-compat`: preserve instruction placement in Responses and compact input history.

## Impact

One shared request normalizer, regression tests, and OpenSpec documentation. No configuration, dependencies, retained state, database migration, or explicit cache-control requirement. The pre-existing messages alias coercion is outside this change.
