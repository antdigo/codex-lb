## Why

A transferred execution waiter can lose its replacement during a preparation
await. Existing submit recovery can then open another physical socket before
the first response.create, bypassing the promised single replacement.

## What Changes

Record that this request already selected its execution-waiter replacement and
refuse further pre-dispatch reconnects at the shared reconnect entry point.
Keep existing recovery policy after an actual response.create attempt.

## Impact

Only the pre-dispatch recovery budget for an execution-waiter transfer changes.
There are no configuration, dependency, native-helper or schema changes.
