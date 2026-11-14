## Why

Claude Code histories can carry function outputs as input_text arrays and
reasoning as explicit reasoning_text content. Both are native Responses input
formats, but the idle socket-age guard currently accepts only string tool
outputs and encrypted reasoning. Complete text histories therefore keep an
aged socket and can hit its lifetime limit after response acceptance.

## What Changes

Recognize complete native text-only output arrays and explicit plaintext
reasoning when deciding whether an idle canonical prompt-cache socket can be
rotated before dispatch. Preserve every input item and all existing ownership,
account, anchor, deadline and incomplete-content fences.

## Impact

Only conservative pre-dispatch socket-age eligibility changes. No replay of
accepted responses, transport-policy switch, payload rewrite, schema,
dependency or native-helper change is introduced.
