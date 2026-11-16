## Why
A self-contained Claude Code request was sent on a 3593.4-second-old upstream WebSocket. It was accepted, then failed at 3600.031 seconds with websocket_connection_limit_reached. The bridge tracks idle TTL but not physical age.

## What Changes
Rotate an eligible, completely unowned, unanchored prompt-cache bridge before a new send when its physical socket is at least 3000 seconds old. Reuse existing registry replacement/epoch and retirement machinery. Retain the old account as a required owner and prohibit owner-dropping HTTP fallback. Keep normal requests and other continuity paths unchanged.

## Impact
No new settings, dependencies, schema, Bifrost or native changes. Scope excludes busy bridges, original continuity references, unknown opaque input and raw downstream WebSocket. A generation lasting an hour can still exceed the upstream limit.
