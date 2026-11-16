# HTTP bridge operation lifecycle

## Why
A cache affinity currently doubles as a physical socket/execution identity. Independent agents serialize behind long responses; socket retirement can strand queued requests, and transport recovery is distributed across readers and stream consumers.

## What Changes
Separate operation controller, physical attempts/generations, continuity lanes and cache-affinity pool. Preserve one public response lifecycle. Permit one proven same-account transport replay before published content, including opaque reasoning only. Limit each compatible per-replica pool to seven ordinary lanes; enforce account stream cap eight with one recovery reserve across initial admission and warm reacquisition. Preserve hard anchors, auth policy, deadlines, durable ownership and exactly-once settlement.

## Impact
Responses HTTP bridge, proxy admission control and tests. No database migration, client protocol change or production deployment as part of source implementation. Direct downstream WebSocket behavior remains compatible.
