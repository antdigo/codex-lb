## Context

The anonymous-output matcher already builds a list of pending created responses. More than one such response makes ownership ambiguous regardless of downstream visibility.

## Decisions

- Return no owner immediately for several created responses, before legacy visible/draining/pre-created fallback.
- Preserve the one-created-owner rule and explicit response-id matching. Leave anonymous error/terminal and vendor telemetry rules unchanged.
- Assert no consumer queue content, no request event accounting, and no archive request owner through the HTTP bridge processing path.

## Risks and rollout

Ambiguous frames remain unavailable until an authoritative terminal envelope can recover owned output. Choosing a visible or draining request would expose unrelated content. The fix adds only a length comparison to the already-built list and retains no memory. It is shared Python matching code and requires no migration, configuration, or native helper rebuild.
