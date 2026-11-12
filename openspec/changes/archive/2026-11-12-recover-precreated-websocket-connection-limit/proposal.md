## Why

The subscription Responses WebSocket backend closes physical connections at its
60-minute lifetime limit. The HTTP bridge currently forwards the explicit limit
error to Claude-compatible clients even when no response was accepted. Idle age
rotation is conservative and cannot replace every owned or busy connection.

## What Changes

Recover one unaccepted HTTP bridge turn from an explicit connection-lifetime
error on a new socket owned by the same account. Retain the original context,
deadline and reservation. Preserve terminal errors once a response is accepted.

## Impact

Only the HTTP bridge's handling of the explicit lifetime error changes. Raw
downstream WebSocket policy, account quotas, schemas and settings stay as they are.
