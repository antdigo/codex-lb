# Audit subscription integration boundaries

## Why
Subscription integration crosses input identity, socket/account ownership and
public response identity. Gaps at these boundaries can trigger persisted-item
lookups, lose item/content/tool lifecycles, charge foreign response usage, or
publish a request beside an owner still finalizing. Client disconnects can leave
startup or keepalive reads alive unless the response owns their cleanup.

## What Changes
Fence explicit foreign response owners before rewriting and settlement; retain
trusted provenance and original failure details for local errors. Repair missing
owned item/content/argument lifecycles from done/terminal snapshots without
replaying visible content. Preserve the accepted terminal outcome after late
frames or exceptions. Normalize IDs at effective classic transport boundaries.
Fence account changes and retirement at pending publication, preserve terminal
execution ownership, and make response-level cleanup cancel and join its readers
even if writing headers fails before body iteration starts.

## Impact
No settings, dependencies, migrations or Bifrost changes. Preserve native wire
contracts, ciphertext, call IDs, cache identity, bounded queues and settlements.
