## Why

A soft prompt-cache HTTP turn can wait for an earlier execution while the
socket retires. Its local pre-dispatch refusal currently escapes to clients
although no response.create for the waiting turn was attempted.

## What Changes

Permit one replacement of the retired bridge for a fully cleaned, unsubmitted,
unanchored soft prompt-cache waiter, preserving its account owner and deadline.
Keep the existing accepted-response and continuity fences.

## Impact

Only HTTP bridge pre-dispatch retirement recovery changes. No settings,
dependency or database changes are needed.
